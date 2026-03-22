#pragma once

#include <string>
#include <string_view>

namespace trading::adapters::exchanges::kalshi {

struct Credentials {
    std::string key_id;
    // Accepts PEM contents or a filesystem path to a PEM file.
    std::string private_key_pem;
};

struct AuthHeaders {
    std::string key_id;
    std::string timestamp_ms;
    std::string signature_base64;
};

class AuthSigner {
  public:
    explicit AuthSigner(Credentials credentials);

    [[nodiscard]] AuthHeaders make_auth_headers(const std::string& method,
                                                const std::string& path) const;
    [[nodiscard]] AuthHeaders
    make_ws_headers(const std::string& ws_path = "/trade-api/ws/v2") const;

    [[nodiscard]] const Credentials& credentials() const;

  private:
    [[nodiscard]] std::string sign_payload_base64(std::string_view payload) const;

    Credentials credentials_;
};

} // namespace trading::adapters::exchanges::kalshi
