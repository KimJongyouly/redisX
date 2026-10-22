// hiredis를 감싼 아주 작은 RAII 래퍼 — 8개 예제가 공유한다. C++에 Redis
// 전용 라이브러리를 새로 끌어올 필요 없이, hiredis C API를 그대로 쓰되
// 연결 수명 관리와 에러 처리를 예외로 바꿔주는 정도의 얇은 층이다.
#ifndef REDISX_EXAMPLE_REDISX_CLIENT_HPP
#define REDISX_EXAMPLE_REDISX_CLIENT_HPP

#include <hiredis/hiredis.h>

#include <cstdarg>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

namespace redisx {

// redisReply*를 감싸서 함수를 빠져나갈 때(예외 포함) 자동으로
// freeReplyObject()하는 스마트 포인터. 멤버 접근은 operator->로.
class Reply {
   public:
    explicit Reply(redisReply* r) : reply_(r, freeReplyObject) {}

    redisReply* operator->() const { return reply_.get(); }
    redisReply* get() const { return reply_.get(); }

    // 배열/상태/벌크 문자열 응답을 사람이 읽기 좋은 "[a, b, c]" 형태로.
    std::string toString() const { return format(reply_.get()); }

   private:
    static std::string format(redisReply* r) {
        if (!r) return "(null)";
        switch (r->type) {
            case REDIS_REPLY_STRING:
            case REDIS_REPLY_STATUS:
                return r->str ? r->str : "";
            case REDIS_REPLY_INTEGER:
                return std::to_string(r->integer);
            case REDIS_REPLY_NIL:
                return "(nil)";
            case REDIS_REPLY_DOUBLE:
                return std::to_string(r->dval);
            case REDIS_REPLY_ARRAY: {
                std::ostringstream out;
                out << "[";
                for (size_t i = 0; i < r->elements; i++) {
                    if (i) out << ", ";
                    out << format(r->element[i]);
                }
                out << "]";
                return out.str();
            }
            default:
                return "(type=" + std::to_string(r->type) + ")";
        }
    }

    std::shared_ptr<redisReply> reply_;
};

class Client {
   public:
    Client(const std::string& host, int port) {
        ctx_ = redisConnect(host.c_str(), port);
        if (ctx_ == nullptr || ctx_->err) {
            std::string msg = ctx_ ? ctx_->errstr : "can't allocate redis context";
            if (ctx_) redisFree(ctx_);
            throw std::runtime_error("redis connection error: " + msg);
        }
    }

    ~Client() {
        if (ctx_) redisFree(ctx_);
    }

    Client(const Client&) = delete;
    Client& operator=(const Client&) = delete;

    // 가변 인자 커맨드 — printf 스타일 포맷 문자열을 그대로 hiredis에
    // 넘긴다("SET %s %s" 등). 커맨드 실패/Redis 에러는 예외로 변환한다.
    Reply command(const char* format, ...) {
        va_list ap;
        va_start(ap, format);
        redisReply* r = static_cast<redisReply*>(redisvCommand(ctx_, format, ap));
        va_end(ap);
        if (r == nullptr) {
            throw std::runtime_error(std::string("command error: ") + ctx_->errstr);
        }
        if (r->type == REDIS_REPLY_ERROR) {
            std::string msg = r->str ? r->str : "unknown error";
            freeReplyObject(r);
            throw std::runtime_error("redis error: " + msg);
        }
        return Reply(r);
    }

    // SUBSCRIBE/PSUBSCRIBE 이후 서버가 이 연결로 비동기로 밀어주는 다음
    // 프레임(구독 확인 응답 자체든, 실제 message/pmessage든) 하나를 읽는다 —
    // `command`와 달리 **새 커맨드를 보내지 않는다**(hiredis의 블로킹
    // `redisGetReply`를 그냥 감싼 것, pub/sub 전용 예제에서만 쓴다).
    Reply nextPush() {
        void* reply = nullptr;
        if (redisGetReply(ctx_, &reply) != REDIS_OK || reply == nullptr) {
            throw std::runtime_error(std::string("pubsub read error: ") + ctx_->errstr);
        }
        return Reply(static_cast<redisReply*>(reply));
    }

    static std::string hostFromEnv() {
        const char* h = std::getenv("REDISX_HOST");
        return h ? h : "127.0.0.1";
    }

    static int portFromEnv() {
        const char* p = std::getenv("REDISX_PORT");
        return p ? std::atoi(p) : 2417;
    }

   private:
    redisContext* ctx_ = nullptr;
};

inline void section(const std::string& title) {
    std::cout << "\n--- " << title << " ---\n";
}

}  // namespace redisx

#endif
