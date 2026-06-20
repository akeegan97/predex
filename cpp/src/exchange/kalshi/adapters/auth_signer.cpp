#include <chrono>

#include <openssl/err.h>
#include <openssl/pem.h>
#include <openssl/rsa.h>
#include <vector>
#include "predex/exchange/kalshi/adapters/auth_signer.hpp"

namespace {
    std::string base64_encode(const unsigned char* data, size_t len){
        if (len > static_cast<size_t>(std::numeric_limits<int>::max())){
            throw std::runtime_error("Signature is too large to base64 encode");
        }
        std::string encoded(4* ((len + 2) / 3), '\0');
        const int written = EVP_EncodeBlock(reinterpret_cast<unsigned char*>(encoded.data()), data, static_cast<int>(len));
        if (written < 0) {
            throw std::runtime_error("EVP_EncodeBlock failed");
        }
        
        encoded.resize(static_cast<size_t>(written));
        
        return encoded;
    }

} // anonymous
namespace predex::exchange::kalshi{
    
    AuthHeaders AuthSigner::make_rest_auth_headers(const RestAuthArguments& args) const{
        const auto timestamp_ms = 
            std::to_string(std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch())
                .count());
        const auto payload = timestamp_ms + args.method + args.path;
        return AuthHeaders{
            .key_id = credentials_.key_id,
            .timestamp_ms = timestamp_ms,
            .signature_base64 = sign_payload_base64(payload),
        };
    }

    std::string AuthSigner::sign_rsa_pss_sha256_base64(std::string_view payload) const{
        if(pkey_ == nullptr){
            throw std::runtime_error(openssl_error_message("Private key not initialized"));
        }

        EVP_MD_CTX* md_ctx = EVP_MD_CTX_new();
        
        if(md_ctx == nullptr){
            throw std::runtime_error(openssl_error_message("Failed to create EVP_MD_CTX"));
        }
        std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_free)> md_ctx_guard(md_ctx, &EVP_MD_CTX_free);
        
        EVP_PKEY_CTX* pkey_ctx = nullptr;
        if(EVP_DigestSignInit(md_ctx_guard.get(), &pkey_ctx, EVP_sha256(), nullptr, pkey_.get()) <= 0){
            throw std::runtime_error(openssl_error_message("EVP_DigestSignInit failed"));
        }
        if(pkey_ctx == nullptr){
            throw std::runtime_error(openssl_error_message("EVP_DigestSignInit returned null EVP_PKEY_CTX"));
        }
        if(EVP_PKEY_CTX_set_rsa_padding(pkey_ctx, RSA_PKCS1_PSS_PADDING) <= 0){
            throw std::runtime_error(openssl_error_message("Failed to set RSA-PSS padding"));
        }
        if(EVP_PKEY_CTX_set_rsa_mgf1_md(pkey_ctx, EVP_sha256()) <= 0){
            throw std::runtime_error(openssl_error_message("Failed to set MGF1 digest"));
        }
        if(EVP_PKEY_CTX_set_rsa_pss_saltlen(pkey_ctx, RSA_PSS_SALTLEN_DIGEST) <= 0){
            throw std::runtime_error(openssl_error_message("Failed to set RSA-PSS salt length"));
        }
        if(EVP_DigestSignUpdate(md_ctx_guard.get(), payload.data(), payload.size()) <= 0){
            throw std::runtime_error(openssl_error_message("EVP_DigestSignUpdate failed"));
        }
        size_t signature_len = 0;
        if(EVP_DigestSignFinal(md_ctx_guard.get(), nullptr, &signature_len) <= 0){
            throw std::runtime_error(openssl_error_message("EVP_DigestSignFinal length query failed"));
        }
        std::vector<unsigned char> signature(signature_len);
        if(EVP_DigestSignFinal(md_ctx_guard.get(), signature.data(), &signature_len) <= 0){
            throw std::runtime_error(openssl_error_message("EVP_DigestSignFinal failed"));
        }

        signature.resize(signature_len);
        return base64_encode(signature.data(), signature.size());
    }

    std::string AuthSigner::sign_payload_base64(std::string_view payload) const{
        return sign_rsa_pss_sha256_base64(payload);
    }

    AuthHeaders AuthSigner::make_ws_headers(const std::string& ws_path) const{
        return make_rest_auth_headers(RestAuthArguments{
            .method = "GET",
            .path = ws_path,
        });
    }
}