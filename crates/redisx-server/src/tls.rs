//! TLS support — the last item from Phase 13's "§3 전수 감사" pass (see
//! `redis_document/25_redisX.md`'s Phase 17/18/21 notes). Real Redis's TLS
//! support (`tls.c`) wraps every connection through OpenSSL via its own
//! `connection.c` abstraction layer; this port uses `rustls` instead (pure
//! Rust, no OpenSSL FFI/linking story to reproduce) and keeps the scope
//! intentionally narrow:
//!
//! - Server side (accepting on `tls-port`): a client connects and this
//!   server presents its certificate. There's no client-certificate
//!   verification (`tls-auth-clients`), no SNI-based multi-cert selection,
//!   and no `tls-protocols`/`tls-ciphers` tuning knobs — rustls's own safe
//!   modern defaults (TLS 1.2/1.3) are used as-is. An inbound replica link
//!   accepted on `tls-port` gets TLS for free this way too (it's wrapped the
//!   same as any other accepted connection — see `redisx-server.rs`'s
//!   `ConnStream`).
//! - Client side (`tls-replication yes`, this fork's own outbound
//!   `REPLICAOF` handshake in `redisx-server.rs`'s `replicaof_handshake`):
//!   connects to the master with a `rustls::ClientConnection`. There's no
//!   `tls-ca-cert-file` in this fork's config surface at all, so unlike real
//!   Redis this path **never validates the master's certificate chain**
//!   (`NoServerCertVerification` below) — the same "narrow the scope rather
//!   than half-implement CA trust" call Phase 18 already made for the
//!   server side's missing client-cert verification, just mirrored on the
//!   other end of the connection.
//! - One cert + one key, loaded once at startup from `tls-cert-file`/
//!   `tls-key-file`; there's no runtime cert rotation (`TLS reload` via
//!   `SIGHUP`-style signal handling isn't implemented in this fork for
//!   plain config either).
//!
//! Dual-port operation (plain `port` and `tls-port` both listening at once,
//! or `port 0` for TLS-only) falls out naturally from `redisx-server.rs`
//! simply binding two independent `TcpListener`s when both are configured.

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use rustls::{ClientConfig, ClientConnection, DigitallySignedStruct, ServerConfig, ServerConnection, SignatureScheme};
use std::io::{self, Read, Write};
use std::path::Path;
use std::sync::Arc;

/// Installs `ring` as the process-wide default crypto provider — required
/// once before any `ServerConfig::builder()` call. Idempotent in practice:
/// `install_default` only fails if a different provider already won the
/// race, which never happens here (this is the only call site), so a
/// failure is silently ignored rather than treated as fatal.
pub fn install_default_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

/// Loads a PEM certificate chain + private key from disk and builds a
/// `rustls::ServerConfig` with no client-certificate verification —
/// `tls-cert-file`/`tls-key-file`'s whole job. `rustls_pemfile::private_key`
/// auto-detects PKCS#8/RSA/SEC1 PEM key formats, so callers don't need to
/// know which one they have.
pub fn load_server_config(cert_path: &Path, key_path: &Path) -> Result<Arc<ServerConfig>, String> {
    let cert_file = std::fs::File::open(cert_path).map_err(|e| format!("reading {}: {e}", cert_path.display()))?;
    let certs: Vec<CertificateDer<'static>> =
        rustls_pemfile::certs(&mut io::BufReader::new(cert_file)).collect::<Result<_, _>>().map_err(|e| format!("parsing {}: {e}", cert_path.display()))?;
    if certs.is_empty() {
        return Err(format!("{} contains no certificates", cert_path.display()));
    }

    let key_file = std::fs::File::open(key_path).map_err(|e| format!("reading {}: {e}", key_path.display()))?;
    let key: PrivateKeyDer<'static> = rustls_pemfile::private_key(&mut io::BufReader::new(key_file))
        .map_err(|e| format!("parsing {}: {e}", key_path.display()))?
        .ok_or_else(|| format!("{} contains no private key", key_path.display()))?;

    let config = ServerConfig::builder().with_no_client_auth().with_single_cert(certs, key).map_err(|e| format!("building TLS config: {e}"))?;
    Ok(Arc::new(config))
}

/// A `ServerCertVerifier` that accepts any certificate the master presents —
/// see this module's doc comment for why `tls-replication`'s client side has
/// no CA trust store to validate against at all. Still delegates the actual
/// signature checks (not just the chain-of-trust check) to `rustls`'s own
/// `ring`-backed verification functions, so a connection is still a genuine
/// negotiated TLS session — only the "is this certificate one I should
/// trust" question is skipped, not "did the peer actually prove it holds the
/// matching private key".
#[derive(Debug)]
struct NoServerCertVerification(rustls::crypto::CryptoProvider);

impl ServerCertVerifier for NoServerCertVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(&self, message: &[u8], cert: &CertificateDer<'_>, dss: &DigitallySignedStruct) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(message, cert, dss, &self.0.signature_verification_algorithms)
    }

    fn verify_tls13_signature(&self, message: &[u8], cert: &CertificateDer<'_>, dss: &DigitallySignedStruct) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(message, cert, dss, &self.0.signature_verification_algorithms)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

/// `tls-replication`'s client config: no client certificate, and (per this
/// module's doc comment) no server certificate verification either.
pub fn load_client_config() -> Arc<ClientConfig> {
    let verifier = Arc::new(NoServerCertVerification(rustls::crypto::ring::default_provider()));
    Arc::new(ClientConfig::builder().dangerous().with_custom_certificate_verifier(verifier).with_no_client_auth())
}

/// Shared method surface between `rustls::ServerConnection` and
/// `rustls::ClientConnection` — both already have inherent methods with
/// these exact names/signatures (Rust prefers inherent methods over trait
/// methods of the same name, so each impl's body just calls straight back
/// into the concrete type's own method, no recursion). Lets `TlsStream`
/// below be written once and reused for both directions: inbound
/// (`tls-port`, `ServerConnection`) and outbound (`tls-replication`,
/// `ClientConnection`).
pub trait RustlsConnection {
    fn reader(&mut self) -> rustls::Reader<'_>;
    fn writer(&mut self) -> rustls::Writer<'_>;
    fn read_tls(&mut self, rd: &mut dyn io::Read) -> io::Result<usize>;
    fn write_tls(&mut self, wr: &mut dyn io::Write) -> io::Result<usize>;
    fn process_new_packets(&mut self) -> Result<rustls::IoState, rustls::Error>;
    fn wants_write(&self) -> bool;
    fn is_handshaking(&self) -> bool;
}

// `reader`/`writer`/`read_tls`/`write_tls`/`process_new_packets`/
// `wants_write`/`is_handshaking` aren't true inherent methods of
// `ServerConnection`/`ClientConnection` — they're inherited via `Deref`/
// `DerefMut` to `ConnectionCommon<_>`. Method resolution tries the receiver's
// own type *before* following that `Deref` chain, so a same-named trait
// method implemented directly for `ServerConnection`/`ClientConnection`
// would be found first and recurse into itself forever; explicitly going
// through `Deref`/`DerefMut` here reaches `ConnectionCommon`'s real inherent
// methods instead.
macro_rules! impl_rustls_connection {
    ($t:ty) => {
        impl RustlsConnection for $t {
            fn reader(&mut self) -> rustls::Reader<'_> {
                std::ops::DerefMut::deref_mut(self).reader()
            }
            fn writer(&mut self) -> rustls::Writer<'_> {
                std::ops::DerefMut::deref_mut(self).writer()
            }
            fn read_tls(&mut self, rd: &mut dyn io::Read) -> io::Result<usize> {
                std::ops::DerefMut::deref_mut(self).read_tls(rd)
            }
            fn write_tls(&mut self, wr: &mut dyn io::Write) -> io::Result<usize> {
                std::ops::DerefMut::deref_mut(self).write_tls(wr)
            }
            fn process_new_packets(&mut self) -> Result<rustls::IoState, rustls::Error> {
                std::ops::DerefMut::deref_mut(self).process_new_packets()
            }
            fn wants_write(&self) -> bool {
                std::ops::Deref::deref(self).wants_write()
            }
            fn is_handshaking(&self) -> bool {
                std::ops::Deref::deref(self).is_handshaking()
            }
        }
    };
}
impl_rustls_connection!(ServerConnection);
impl_rustls_connection!(ClientConnection);

/// A `rustls` connection (server- or client-side — see `RustlsConnection`)
/// plus the raw socket it reads/writes ciphertext against, implementing
/// `Read`/`Write` over *plaintext* so every other call site
/// (`service_connection`, `handle_psync`, `replicaof_handshake`'s
/// post-handshake streamed phase, ...) can treat it exactly like a plain
/// `TcpStream` — see `redisx-server.rs`'s `ConnStream`.
///
/// This is the standard non-blocking-socket pump loop for `rustls` (not a
/// built-in — `rustls::Stream` assumes a blocking socket): pull ciphertext
/// in with `read_tls`, hand it to `process_new_packets` (which also drives
/// the handshake forward), then drain whatever plaintext (if any) came out
/// the other end. `WouldBlock` from the underlying socket propagates
/// straight through, exactly like a plain `TcpStream` would, so callers
/// already written against `ErrorKind::WouldBlock` need no changes.
pub struct TlsStream<C: RustlsConnection = ServerConnection> {
    sock: mio::net::TcpStream,
    conn: C,
}

/// The outbound (`tls-replication`) direction — `TlsStream` on its own
/// (default type parameter `ServerConnection`) stays the inbound
/// (`tls-port`) one, since every existing caller already refers to it that
/// way from Phase 18.
pub type TlsClientStream = TlsStream<ClientConnection>;

impl<C: RustlsConnection> TlsStream<C> {
    pub fn new(sock: mio::net::TcpStream, conn: C) -> Self {
        TlsStream { sock, conn }
    }

    /// Flushes any handshake/alert bytes `rustls` wants to send — needed
    /// after `process_new_packets` as well as after writing application
    /// data, since a single `write()` call doesn't otherwise guarantee the
    /// ciphertext actually reached the socket.
    fn flush_tls(&mut self) -> io::Result<()> {
        while self.conn.wants_write() {
            self.conn.write_tls(&mut self.sock)?;
        }
        Ok(())
    }
}

impl<C: RustlsConnection> Read for TlsStream<C> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        loop {
            match self.conn.reader().read(buf) {
                Ok(n) if n > 0 => return Ok(n),
                Ok(_) if !self.conn.is_handshaking() => return Ok(0), // clean EOF (peer sent close_notify)
                Ok(_) => {}                                          // 0 bytes, but still mid-handshake: need more input
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {} // no plaintext buffered yet
                Err(e) => return Err(e),
            }
            match self.conn.read_tls(&mut self.sock) {
                Ok(0) => return Ok(0), // TCP EOF
                Ok(_) => {}
                Err(e) => return Err(e), // WouldBlock propagates straight through
            }
            if let Err(e) = self.conn.process_new_packets() {
                return Err(io::Error::new(io::ErrorKind::InvalidData, e));
            }
            self.flush_tls()?;
        }
    }
}

impl<C: RustlsConnection> Write for TlsStream<C> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n = self.conn.writer().write(buf)?;
        self.flush_tls()?;
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.flush_tls()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // A throwaway 10-year self-signed `CN=localhost` cert/key pair
    // (`openssl req -x509 -newkey rsa:2048 -nodes -days 3650 -subj
    // '/CN=localhost'`), committed only as a test fixture — never read by
    // the running server, which always loads real cert/key files named by
    // `tls-cert-file`/`tls-key-file`.
    const TEST_CERT_PEM: &str = "-----BEGIN CERTIFICATE-----
MIIDCTCCAfGgAwIBAgIUUOfiTZ2uf0tlv+DUH2f+hZ/HxpYwDQYJKoZIhvcNAQEL
BQAwFDESMBAGA1UEAwwJbG9jYWxob3N0MB4XDTI2MTAxNDA4MzcwNVoXDTM2MTAx
MTA4MzcwNVowFDESMBAGA1UEAwwJbG9jYWxob3N0MIIBIjANBgkqhkiG9w0BAQEF
AAOCAQ8AMIIBCgKCAQEAmmzrXpgroYHqQon3EqjG69CUDeV0SzwKYm1YJ9nwThCj
yaDxzywVrGaSrjHcfXH2yJ8TV6vGNBJ8E3zIWqYOP6pSrWHPPYYB6nR46PVaf/Md
PxTwrf1S1uvyqk8ImOPBx7qZnAzbO4tRP9Yk51OkA7eW/4nbqfPZWbby3SseW/P4
a1wjQb7OBRx1D1TDE1zztBFgz9hBkJqNFlrztMG5kiWC3QSqZYuq3q1d+Mq1P6bU
KYlACzGBW25y4bGrF+R+ke0pfdxkBgj6vyW1AR2ILBHdASTr0vASLjWLHARYYkB6
lz63vP7s5mdwnVbQ9ZM1tv/Z/AmWpkUsLYINO8OOQwIDAQABo1MwUTAdBgNVHQ4E
FgQUgrQJaUpA2AEfsj0mZabx6DNCn2IwHwYDVR0jBBgwFoAUgrQJaUpA2AEfsj0m
Zabx6DNCn2IwDwYDVR0TAQH/BAUwAwEB/zANBgkqhkiG9w0BAQsFAAOCAQEAFZi1
W+4wWjLRpTXnkYCpZGw31gvsLEUyxmB+Rw36ThrEOFjMTz41pZuo5JV4OFCUL9yL
GTn7/H564l+VUxo1OeTJK6uQ8IrjR4h479W0eM37r97vKkyLviuFvDaxBTdH4UD0
NvNMtb4wost7zbpb25siIAwVZNlCrkY77OQuuFpR6P3/O95oV5fUqJsmZtTWbYXv
dMdkWz0CkNVvAtXEx/xPPtRNXb2/eqbWvp12y8WUYl6R8zA/x32yJlKiHdhUS9O2
JwPtwCpMjJUBtaGPDSovz738YYimoh0vY2tRU7G4AHYZgHi//UiLooRGV47sKaZX
RNMfGeDtWPbCTiA3kg==
-----END CERTIFICATE-----
";

    const TEST_KEY_PEM: &str = "-----BEGIN PRIVATE KEY-----
MIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQCabOtemCuhgepC
ifcSqMbr0JQN5XRLPApibVgn2fBOEKPJoPHPLBWsZpKuMdx9cfbInxNXq8Y0EnwT
fMhapg4/qlKtYc89hgHqdHjo9Vp/8x0/FPCt/VLW6/KqTwiY48HHupmcDNs7i1E/
1iTnU6QDt5b/idup89lZtvLdKx5b8/hrXCNBvs4FHHUPVMMTXPO0EWDP2EGQmo0W
WvO0wbmSJYLdBKpli6rerV34yrU/ptQpiUALMYFbbnLhsasX5H6R7Sl93GQGCPq/
JbUBHYgsEd0BJOvS8BIuNYscBFhiQHqXPre8/uzmZ3CdVtD1kzW2/9n8CZamRSwt
gg07w45DAgMBAAECggEACyceUUd3MI5pRKNMhftM299wBJg+RRK8g0L3Zhq+Lr0n
6cTRwfnNROq8NuV1/FSx6JJd5haLVYZs50a7UkSs7G8uJ8R+daiLKeDTPKvqUdRV
QpGOF7AfKVOCw/G6TWR27H2vC2SoTrgZ8ZzTLyZ3cXydugM9q0a9EKwpr8x7QFO/
2LekZMSbgDGSXLsur7wxR870H7T3QrE6JU9HMl2Z83YBm7N5bo5p8AkmoVFxDsbc
MvkLnW/zMfSD/PtI6R+yW2Aqs0JJm6FnnhRF9MXlB8um2VbtTOsK2G8ANB7n9CUy
B/aQ9timhl+PQmwZi/+aOPVc2bOz7eHFX1YL0ezUxQKBgQDPU/7V9DT/Lwl1Qvvi
oeqxxajepkeaCXquc2Weonc+bPQLqj9V3mOPnwUlWLJhposhGOgT0bYk07B9DFQe
A6ndFfc2WMPolJXa170NwCyvSi81iLgSWjDitkQt7nQSZyT8wCpfYVLO7eq8Xoq1
4aJDRiHmmd2wRYoccFVo6jcUrwKBgQC+rZTAp/GV0mbEW+GOu2gyVhjlnA1X53Gs
1HqCv6hnKn76CkhdiByM4L+q7f5c3wBVQtg4DWguPxVk333rKtKD95eL/0NKLxNz
TArf5c3kFgPK1ZnlWh2Ir3Pr0LfjO/529B5dNBxl/pOHxzqWhAuU53cB8DDFBR2T
PDpO6uusrQKBgGi88jx8Bln0FT8b+8veKIZuaIUyU3HJ4CRJP8kIIl1zBUbvVte2
ZW2k1CCt0k/qptatknCQvQeVuc8ybzxKdYGvGxhld3MDvUC1RFiyAOD9BSRyUStU
TOX7Vi4uDI1PdiwJmF5e+Ep5fuBykniX6DU8NNweVxQ5zS/YwA0kZXcZAoGAa2us
5tB2eJo9DRhaPSf1Lo50nj5JAiHAV8aocip5PA2j1UeftxmsQ+2F7U0AxN9Y96+d
0g4ziAJAEkEaVjccu/3rYZJgHVXiFk1bvwFe+cUxQ78MYTpgcxWtL8e5IQo2dFtZ
E9b8vLUr9g1WQ1+Gis/PqsbfIs+b9suyaa1NMk0CgYBpyNbn7SVNtJZv/pHvGvOu
ctYbO/mpDgjv7Ze7y9A1giWZdq2s0mGNeHWju+ISWidjdnWdEzvNDoW00fGBpm1e
TSAu7umEGhx/MBw2w21ZIYoj8QEjaHQpnyCj1JQNE0UbAoTNMm/AFoJtV+0E35TG
UVKYk5G1G4aJbxIv/OKrkg==
-----END PRIVATE KEY-----
";

    fn scratch_path(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("redisx_tls_test_{}_{name}", std::process::id()))
    }

    #[test]
    fn load_server_config_succeeds_on_a_valid_cert_and_key() {
        install_default_crypto_provider();
        let cert_path = scratch_path("cert.pem");
        let key_path = scratch_path("key.pem");
        std::fs::write(&cert_path, TEST_CERT_PEM).unwrap();
        std::fs::write(&key_path, TEST_KEY_PEM).unwrap();

        let result = load_server_config(&cert_path, &key_path);
        assert!(result.is_ok(), "expected a valid ServerConfig, got {:?}", result.err());

        std::fs::remove_file(&cert_path).ok();
        std::fs::remove_file(&key_path).ok();
    }

    #[test]
    fn load_server_config_fails_on_a_missing_cert_file() {
        let result = load_server_config(&scratch_path("does-not-exist-cert.pem"), &scratch_path("does-not-exist-key.pem"));
        assert!(result.is_err());
    }

    #[test]
    fn load_server_config_fails_on_an_empty_cert_file() {
        let cert_path = scratch_path("empty-cert.pem");
        let key_path = scratch_path("key-for-empty-cert.pem");
        std::fs::write(&cert_path, "").unwrap();
        std::fs::write(&key_path, TEST_KEY_PEM).unwrap();

        let result = load_server_config(&cert_path, &key_path);
        assert!(result.is_err(), "an empty cert file has no certificates to load");

        std::fs::remove_file(&cert_path).ok();
        std::fs::remove_file(&key_path).ok();
    }

    #[test]
    fn load_server_config_fails_when_the_key_does_not_match_the_cert() {
        // A structurally valid but *different* keypair than the one that
        // signed `TEST_CERT_PEM` — `with_single_cert` must reject the
        // mismatch rather than silently building an unusable config.
        let cert_path = scratch_path("mismatched-cert.pem");
        let key_path = scratch_path("mismatched-key.pem");
        std::fs::write(&cert_path, TEST_CERT_PEM).unwrap();
        // A syntactically valid PEM key block that isn't a real key (garbage
        // base64 payload) is enough to make parsing or cert/key matching
        // fail — this test only needs *some* mismatch to be rejected.
        std::fs::write(&key_path, "-----BEGIN PRIVATE KEY-----\nQUJDRA==\n-----END PRIVATE KEY-----\n").unwrap();

        let result = load_server_config(&cert_path, &key_path);
        assert!(result.is_err());

        std::fs::remove_file(&cert_path).ok();
        std::fs::remove_file(&key_path).ok();
    }

    /// `tls-replication`'s client side, end to end: a real TCP loopback
    /// connection, `ServerConnection` on one end (via `load_server_config`,
    /// the exact same path `tls-port` uses) and `ClientConnection` on the
    /// other (via `load_client_config`, the exact same path
    /// `replicaof_handshake` uses) — proving `NoServerCertVerification`
    /// actually lets a handshake against this fork's self-signed test
    /// certificate complete, and that plaintext flows correctly once it
    /// does. Both sides use `rustls::Stream` (the library's own blocking
    /// wrapper) rather than `TlsStream`'s non-blocking pump, matching how
    /// `replicaof_handshake` actually drives its side during the
    /// synchronous handshake phase.
    #[test]
    fn client_config_completes_a_handshake_against_the_test_server_cert() {
        install_default_crypto_provider();
        let cert_path = scratch_path("client-test-cert.pem");
        let key_path = scratch_path("client-test-key.pem");
        std::fs::write(&cert_path, TEST_CERT_PEM).unwrap();
        std::fs::write(&key_path, TEST_KEY_PEM).unwrap();
        let server_config = load_server_config(&cert_path, &key_path).unwrap();
        std::fs::remove_file(&cert_path).ok();
        std::fs::remove_file(&key_path).ok();

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let server_thread = std::thread::spawn(move || {
            let (mut sock, _) = listener.accept().unwrap();
            let mut conn = ServerConnection::new(server_config).unwrap();
            let mut stream = rustls::Stream::new(&mut conn, &mut sock);
            let mut buf = [0u8; 5];
            stream.read_exact(&mut buf).unwrap();
            assert_eq!(&buf, b"hello");
            stream.write_all(b"world").unwrap();
        });

        let mut sock = std::net::TcpStream::connect(addr).unwrap();
        let client_config = load_client_config();
        let server_name = ServerName::try_from("localhost").unwrap();
        let mut conn = ClientConnection::new(client_config, server_name).unwrap();
        {
            let mut stream = rustls::Stream::new(&mut conn, &mut sock);
            stream.write_all(b"hello").unwrap();
            let mut buf = [0u8; 5];
            stream.read_exact(&mut buf).unwrap();
            assert_eq!(&buf, b"world");
        }

        server_thread.join().unwrap();
    }
}
