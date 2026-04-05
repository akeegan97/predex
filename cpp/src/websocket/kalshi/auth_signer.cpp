#include "predex/websocket/kalshi/auth_signer.hpp"

#include <array>
#include <chrono>
#include <fstream>
#include <limits>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <utility>
#include <vector>

#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/rsa.h>

namespace predex::websocket::kalshi {
namespace {

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

std::string read_file_contents(const std::string& path) {
    std::ifstream pem_file(path, std::ios::binary);
    if (!pem_file) {
        throw std::runtime_error("Failed to open PEM file: " + path);
    }
    std::stringstream buffer;
    buffer << pem_file.rdbuf();
    return buffer.str();
}

std::string resolve_private_key_pem(std::string_view private_key_pem_or_path) {
    std::string value{private_key_pem_or_path};
    if (value.empty()) {
        throw std::runtime_error("Kalshi private key is empty");
    }
    if (value.find("-----BEGIN") != std::string::npos) {
        return value;
    }
    return read_file_contents(value);
}

std::string base64_encode(const unsigned char* data, size_t len) {
    if (len > static_cast<size_t>(std::numeric_limits<int>::max())) {
        throw std::runtime_error("Signature is too large to base64 encode");
    }

    std::string encoded(4 * ((len + 2) / 3), '\0');
    const int written = EVP_EncodeBlock(reinterpret_cast<unsigned char*>(encoded.data()), data,
                                        static_cast<int>(len));
    if (written < 0) {
        throw std::runtime_error("Failed to base64 encode signature");
    }
    encoded.resize(static_cast<size_t>(written));
    return encoded;
}

struct SignatureInput {
    std::string_view private_key_pem;
    std::string_view payload_message;
};

std::string sign_rsa_pss_sha256_base64(const SignatureInput& input) {
    const std::string pem_content{input.private_key_pem};
    BIO* bio = BIO_new_mem_buf(pem_content.data(), static_cast<int>(pem_content.size()));
    if (bio == nullptr) {
        throw std::runtime_error(openssl_error_message("Failed to create BIO for private key"));
    }
    std::unique_ptr<BIO, decltype(&BIO_free)> bio_guard(bio, BIO_free);

    EVP_PKEY* pkey = PEM_read_bio_PrivateKey(bio_guard.get(), nullptr, nullptr, nullptr);
    if (pkey == nullptr) {
        throw std::runtime_error(openssl_error_message("Failed to parse private key PEM"));
    }
    std::unique_ptr<EVP_PKEY, decltype(&EVP_PKEY_free)> pkey_guard(pkey, EVP_PKEY_free);

    EVP_MD_CTX* md_ctx = EVP_MD_CTX_new();
    if (md_ctx == nullptr) {
        throw std::runtime_error(openssl_error_message("Failed to create digest context"));
    }
    std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_free)> md_ctx_guard(md_ctx, EVP_MD_CTX_free);

    EVP_PKEY_CTX* pkey_ctx = nullptr;
    if (EVP_DigestSignInit(md_ctx_guard.get(), &pkey_ctx, EVP_sha256(), nullptr,
                           pkey_guard.get()) <= 0) {
        throw std::runtime_error(openssl_error_message("EVP_DigestSignInit failed"));
    }
    if (pkey_ctx == nullptr) {
        throw std::runtime_error("EVP_DigestSignInit returned null EVP_PKEY_CTX");
    }

    if (EVP_PKEY_CTX_set_rsa_padding(pkey_ctx, RSA_PKCS1_PSS_PADDING) <= 0) {
        throw std::runtime_error(openssl_error_message("Failed to set RSA-PSS padding"));
    }
    if (EVP_PKEY_CTX_set_rsa_mgf1_md(pkey_ctx, EVP_sha256()) <= 0) {
        throw std::runtime_error(openssl_error_message("Failed to set MGF1 digest"));
    }
    if (EVP_PKEY_CTX_set_rsa_pss_saltlen(pkey_ctx, RSA_PSS_SALTLEN_DIGEST) <= 0) {
        throw std::runtime_error(openssl_error_message("Failed to set RSA-PSS salt length"));
    }

    if (EVP_DigestSignUpdate(md_ctx_guard.get(), input.payload_message.data(),
                             input.payload_message.size()) <= 0) {
        throw std::runtime_error(openssl_error_message("EVP_DigestSignUpdate failed"));
    }

    size_t signature_len = 0;
    if (EVP_DigestSignFinal(md_ctx_guard.get(), nullptr, &signature_len) <= 0) {
        throw std::runtime_error(openssl_error_message("EVP_DigestSignFinal length query failed"));
    }

    std::vector<unsigned char> signature(signature_len);
    if (EVP_DigestSignFinal(md_ctx_guard.get(), signature.data(), &signature_len) <= 0) {
        throw std::runtime_error(openssl_error_message("EVP_DigestSignFinal failed"));
    }

    return base64_encode(signature.data(), signature_len);
}

} // namespace

AuthSigner::AuthSigner(Credentials credentials) : credentials_(std::move(credentials)) {}

AuthHeaders AuthSigner::make_auth_headers(const std::string& method,
                                          const std::string& path) const {
    const auto timestamp_ms =
        std::to_string(std::chrono::duration_cast<std::chrono::milliseconds>(
                           std::chrono::system_clock::now().time_since_epoch())
                           .count());

    const auto payload = timestamp_ms + method + path;
    return AuthHeaders{
        .key_id = credentials_.key_id,
        .timestamp_ms = timestamp_ms,
        .signature_base64 = sign_payload_base64(payload),
    };
}

AuthHeaders AuthSigner::make_ws_headers(const std::string& ws_path) const {
    return make_auth_headers("GET", ws_path);
}

const Credentials& AuthSigner::credentials() const { return credentials_; }

std::string AuthSigner::sign_payload_base64(std::string_view payload) const {
    const std::string private_key_pem = resolve_private_key_pem(credentials_.private_key_pem);
    return sign_rsa_pss_sha256_base64(
        SignatureInput{.private_key_pem = private_key_pem, .payload_message = payload});
}

} // namespace predex::websocket::kalshi
