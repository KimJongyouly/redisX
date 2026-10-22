"""RedisDF(DF.* 모듈) 쿼리 결과를 polars DataFrame으로 동적 매핑해주는 래퍼.

사용 예:
    my_df = redis_df('df_test')
    df = my_df.query("SELECT kapt_name, kapt_addr FROM self LIMIT 10")

    

    # 로긴 분석 
    #load 
    redisx-cli -x DF.FROMPARQUET df_login < ~/Downloads/login.parquet
    

from redis_df import redis_df 
import polars as pl

pl.Config.set_tbl_rows(-1)
pl.Config.set_tbl_cols(-1)

    
    my_login = redis_df('df_login')

    my_login.query('select dt, count(distinct user_idx) as cnt from self group by dt order by dt', profile=True)
    my_login.query('select dt, count(user_idx) as cnt from self group by dt order by dt', profile=True)


    my_login.dau(date_column='dt', user_column='user_idx', start_date='2026-09-01', end_date='2026-09-15', profile=True)
    my_login.pv(date_column='dt', start_date='2026-09-01', end_date='2026-09-15', profile=True)
    my_login.retention(date_column='dt', user_column='user_idx', start_date='2026-09-01', end_date='2026-09-15', profile=True)
    my_login.retention_ratio(date_column='dt', user_column='user_idx', start_date='2026-09-01', end_date='2026-09-15', profile=True)


"""

import re
import time

import redis
import polars as pl

_SELECT_RE = re.compile(r"select\s+(.*?)\s+from\s", re.IGNORECASE | re.DOTALL)
_AS_RE = re.compile(r"\s+as\s+(.+)$", re.IGNORECASE | re.DOTALL)
_SIMPLE_COL_RE = re.compile(r"[\w.]+")
_DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}:\d{2})?")


def _decode_cell(v):
    return v.decode("utf-8") if isinstance(v, bytes) else v


def _split_top_level(s, sep=","):
    """괄호/따옴표 안의 콤마는 무시하고 최상위 레벨에서만 분리."""
    parts = []
    depth = 0
    quote = None
    buf = []
    for ch in s:
        if quote:
            buf.append(ch)
            if ch == quote:
                quote = None
            continue
        if ch in ("'", '"'):
            quote = ch
            buf.append(ch)
            continue
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if ch == sep and depth == 0:
            parts.append("".join(buf))
            buf = []
        else:
            buf.append(ch)
    if buf:
        parts.append("".join(buf))
    return parts


def infer_result_columns(sql, table_columns):
    """SELECT 절을 파싱해서 쿼리 결과에 대응하는 컬럼명 리스트를 추정."""
    m = _SELECT_RE.search(sql)
    if not m:
        return None

    columns = []
    for item in _split_top_level(m.group(1).strip()):
        item = item.strip()
        if item == "*":
            columns.extend(table_columns)
            continue

        m_as = _AS_RE.search(item)
        if m_as:
            columns.append(m_as.group(1).strip().strip("`\"[]"))
            continue

        if _SIMPLE_COL_RE.fullmatch(item):
            columns.append(item.split(".")[-1])
        else:
            columns.append(re.sub(r"\s+", " ", item))

    return columns


class redis_df:
    def __init__(self, key, host="localhost", port=2417, **redis_kwargs):
        self.key = key
        self._redis = redis.Redis(host=host, port=port, **redis_kwargs)
        self.last_profile = {}

    def columns(self):
        return [_decode_cell(c) for c in self._redis.execute_command("DF.COLUMNS", self.key)]

    def query(self, sql, profile=False):
        t0 = time.perf_counter()
        raw_rows = self._redis.execute_command("DF.QUERY", self.key, sql)
        t1 = time.perf_counter()

        rows = [[_decode_cell(v) for v in row] for row in raw_rows]
        t2 = time.perf_counter()

        table_columns = self.columns()
        t3 = time.perf_counter()

        width = len(rows[0]) if rows else None
        cols = infer_result_columns(sql, table_columns)

        if width is not None and (not cols or len(cols) != width):
            cols = [f"col_{i}" for i in range(width)]
        elif width is None and not cols:
            cols = []
        t4 = time.perf_counter()

        if not rows:
            df = pl.DataFrame(schema=cols)
        else:
            df = pl.DataFrame(rows, schema=cols, orient="row")
        t5 = time.perf_counter()

        self.last_profile = {
            "df_query": t1 - t0,
            "decode": t2 - t1,
            "df_columns": t3 - t2,
            "infer_columns": t4 - t3,
            "build_dataframe": t5 - t4,
            "total": t5 - t0,
        }
        if profile:
            print(
                "[redis_df] "
                + " | ".join(f"{k}: {v * 1000:.2f}ms" for k, v in self.last_profile.items())
            )
            print(f"[redis_df] sql: {sql}")
            print()

        return df

    def dau(self, *, date_column, user_column, start_date, end_date, profile=False):
        """일자별 순사용자수(DAU)를 집계.

        date_column, user_column은 식별자 형식(영숫자/밑줄/점)만 허용하고,
        start_date, end_date는 'YYYY-MM-DD' 또는 'YYYY-MM-DD HH:MM:SS' 형식만 허용해
        SQL 인젝션을 차단한다.
        """
        if not _SIMPLE_COL_RE.fullmatch(date_column):
            raise ValueError(f"invalid date_column: {date_column!r}")
        if not _SIMPLE_COL_RE.fullmatch(user_column):
            raise ValueError(f"invalid user_column: {user_column!r}")
        if not _DATE_RE.fullmatch(start_date):
            raise ValueError(f"invalid start_date: {start_date!r}")
        if not _DATE_RE.fullmatch(end_date):
            raise ValueError(f"invalid end_date: {end_date!r}")

        sql = (
            f"SELECT {date_column}, COUNT(DISTINCT {user_column}) AS dau "
            f"FROM self "
            f"WHERE {date_column} >= '{start_date}' AND {date_column} <= '{end_date}' "
            f"GROUP BY {date_column} "
            f"ORDER BY {date_column}"
        )
        return self.query(sql, profile=profile)

    def pv(self, *, date_column, start_date, end_date, profile=False):
        """일자별 조회수(PV)를 집계.

        date_column은 식별자 형식(영숫자/밑줄/점)만 허용하고,
        start_date, end_date는 'YYYY-MM-DD' 또는 'YYYY-MM-DD HH:MM:SS' 형식만 허용해
        SQL 인젝션을 차단한다.
        """
        if not _SIMPLE_COL_RE.fullmatch(date_column):
            raise ValueError(f"invalid date_column: {date_column!r}")
        if not _DATE_RE.fullmatch(start_date):
            raise ValueError(f"invalid start_date: {start_date!r}")
        if not _DATE_RE.fullmatch(end_date):
            raise ValueError(f"invalid end_date: {end_date!r}")

        sql = (
            f"""SELECT {date_column}, COUNT(*) AS pv
            FROM self 
            WHERE {date_column} >= '{start_date}' AND {date_column} <= '{end_date}' 
            GROUP BY {date_column} 
            ORDER BY {date_column}"""
        )
        return self.query(sql, profile=profile)

    def retention(self, *, date_column, user_column, start_date, end_date, profile=False):
        """코호트 기준 잔존율(Retention) 행렬을 집계.

        date_column, user_column은 식별자 형식(영숫자/밑줄/점)만 허용하고,
        start_date, end_date는 'YYYY-MM-DD' 또는 'YYYY-MM-DD HH:MM:SS' 형식만 허용해
        SQL 인젝션을 차단한다. 반환값은 cohort 일자(date_column)를 행으로,
        경과일수(D, D+1, D+2, ...)를 열로 갖는 polars DataFrame이다.
        """
        if not _SIMPLE_COL_RE.fullmatch(date_column):
            raise ValueError(f"invalid date_column: {date_column!r}")
        if not _SIMPLE_COL_RE.fullmatch(user_column):
            raise ValueError(f"invalid user_column: {user_column!r}")
        if not _DATE_RE.fullmatch(start_date):
            raise ValueError(f"invalid start_date: {start_date!r}")
        if not _DATE_RE.fullmatch(end_date):
            raise ValueError(f"invalid end_date: {end_date!r}")

        sql = (
            f"""SELECT cohort_date, compare_date, COUNT(DISTINCT uid) AS retained FROM (
              SELECT t1.{date_column} AS cohort_date, t2.{date_column} AS compare_date, t1.{user_column} AS uid 
              FROM self t1 JOIN self t2 ON t1.{user_column} = t2.{user_column} 
              WHERE t1.{date_column} BETWEEN '{start_date}' AND '{end_date}' 
                AND t2.{date_column} BETWEEN '{start_date}' AND '{end_date}' 
                AND t2.{date_column} >= t1.{date_column} 
            ) sub GROUP BY cohort_date, compare_date ORDER BY cohort_date, compare_date"""
        )
        pairs = self.query(sql, profile=profile)
        
        if pairs.is_empty():
            return pl.DataFrame()

        pairs = pairs.with_columns(
            (
                pl.col("compare_date").str.to_date(strict=False)
                - pl.col("cohort_date").str.to_date(strict=False)
            )
            .dt.total_days()
            .alias("offset")
        )

        max_offset = pairs["offset"].max()
        if max_offset is None:
            raise ValueError(f"{date_column} 값을 날짜로 파싱하지 못했습니다")
        max_offset = int(max_offset)

        matrix = pairs.pivot(on="offset", index="cohort_date", values="retained", aggregate_function="first")

        for i in range(max_offset + 1):
            col = str(i)
            if col not in matrix.columns:
                matrix = matrix.with_columns(pl.lit(None, dtype=pl.Int64).alias(col))

        ordered_columns = ["cohort_date"] + [str(i) for i in range(max_offset + 1)]
        matrix = matrix.select(ordered_columns)

        rename_map = {"cohort_date": date_column}
        rename_map.update({str(i): ("D" if i == 0 else f"D+{i}") for i in range(max_offset + 1)})
        return matrix.rename(rename_map).sort(date_column)

    def retention_ratio(self, *, date_column, user_column, start_date, end_date, profile=False):
        """retention() 결과를 D=100 기준 비율(%)로 변환.

        D는 100으로 고정하고, D+n = round(D+n / D * 100, 1)로 계산한다.
        D가 0이거나 비어있는 코호트는 비율을 계산할 수 없어 null로 남긴다.
        """
        matrix = self.retention(
            date_column=date_column,
            user_column=user_column,
            start_date=start_date,
            end_date=end_date,
            profile=profile,
        )
        if matrix.is_empty():
            return matrix

        day_columns = [c for c in matrix.columns if c != date_column]
        if "D" not in day_columns:
            raise ValueError("retention 결과에 D 컬럼이 없습니다")

        has_d = pl.col("D").is_not_null() & (pl.col("D") != 0)
        exprs = [pl.col(date_column)]
        for col in day_columns:
            if col == "D":
                exprs.append(pl.when(has_d).then(pl.lit(100.0)).otherwise(None).alias("D"))
            else:
                exprs.append(
                    pl.when(has_d & pl.col(col).is_not_null())
                    .then((pl.col(col) / pl.col("D") * 100).round(1))
                    .otherwise(None)
                    .alias(col)
                )

        return matrix.select(exprs)


def section(title):
    print(f"\n[{datetime.now()}] {title}")


if __name__ == "__main__": 
    from datetime import datetime 
    pl.Config.set_tbl_rows(-1)
    pl.Config.set_tbl_cols(-1)

    v_profile = True 

    section("Assert DataFrame Class")
    my_login = redis_df('df_login')

    section("Querying")
    print(my_login.query('select dt, count(distinct user_idx) as cnt from self group by dt order by dt', profile=v_profile))
    print(my_login.query('select dt, count(user_idx) as cnt from self group by dt order by dt', profile=v_profile))

    section("Dau/Pv/Retention")
    print(my_login.dau(date_column='dt', user_column='user_idx', start_date='2026-09-01', end_date='2026-09-15', profile=v_profile))
    print(my_login.pv(date_column='dt', start_date='2026-09-01', end_date='2026-09-15', profile=v_profile))
    print(my_login.retention(date_column='dt', user_column='user_idx', start_date='2026-09-01', end_date='2026-09-15', profile=v_profile))
    print(my_login.retention_ratio(date_column='dt', user_column='user_idx', start_date='2026-09-01', end_date='2026-09-15', profile=v_profile))

