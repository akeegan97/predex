#pragma once 

#include <string>
#include <memory>
#include <openssl/evp.h>
#include <openssl/err.h>
#include <openssl/pem.h>
#include <openssl/rsa.h>
#include <fstream>
#include <sstream>
#include <array>
#include <string_view>


namespace {
    std::string resolve_private_key_pem(std::string_view private_key_pem_or_path){
        std::string value{private_key_pem_or_path};
        if (value.empty()) {
            throw std::runtime_error("Kalshi private key is empty");
        }
        if (value.find("-----BEGIN") != std::string::npos) {
            return value;
        }
        std::ifstream pem_file(value, std::ios::binary);
        if (!pem_file) {
            throw std::runtime_error("Failed to open PEM file: " + value);
        }
        std::stringstream buffer;
        buffer << pem_file.rdbuf();
        return buffer.str();
    }

    std::string openssl_error_message(const std::string& context) {
        constexpr size_t kOpenSslErrorBufferSize = 256;
        const unsigned long code = ERR_get_error();
        if (code == 0) {
            return context;
        }

        std::array<char, kOpenSslErrorBufferSize> buffer{};
        ERR_error_string_n(code, buffer.data(), buffer.size());
        return context + ": " + std::string(buffer.data());
    }
}
namespace predex::exchange::kalshi{
    
    struct Credentials{
        std::string key_id;
        std::string private_key_pem;
    };

    struct AuthHeaders{
        std::string key_id;
        std::string timestamp_ms;
        std::string signature_base64;
    };

    struct RestAuthArguments{
        std::string method;
        std::string path;
    };


    class AuthSigner{
        public: 
            explicit AuthSigner(Credentials credentials) : credentials_(std::move(credentials)) {
                const std::string private_key_pem = resolve_private_key_pem(credentials_.private_key_pem);
                BIO* bio = BIO_new_mem_buf(private_key_pem.data(), static_cast<int>(private_key_pem.size()));
                if (bio == nullptr) {
                    throw std::runtime_error("Failed to create BIO for private key");
                }
                std::unique_ptr<BIO, decltype(&BIO_free)> bio_guard(bio, &BIO_free);

                EVP_PKEY* pkey = PEM_read_bio_PrivateKey(bio_guard.get(), nullptr, nullptr, nullptr);
                if (pkey == nullptr) {
                    throw std::runtime_error(openssl_error_message("Failed to parse PEM private key"));
                }
                pkey_.reset(pkey);
            };

            [[nodiscard]] AuthHeaders make_rest_auth_headers(const RestAuthArguments& args) const;
            [[nodiscard]] AuthHeaders make_ws_headers(const std::string& ws_path = "/trade-api/ws/v2") const;

            [[nodiscard]] const Credentials& credentials() const{
                return credentials_;
            }
        private:
            [[nodiscard]] std::string sign_payload_base64(std::string_view payload) const;
            [[nodiscard]] std::string sign_rsa_pss_sha256_base64(std::string_view payload) const;
            Credentials credentials_;
            std::unique_ptr<EVP_PKEY, decltype(&EVP_PKEY_free)> pkey_{nullptr, &EVP_PKEY_free};

    };
}