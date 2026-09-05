#pragma once
#include <string>
#include <unistd.h>
namespace predex::socket{

    class SocketPathGuard{
        public:
            //NOLINTNEXTLINE -> intentionally not using std::move since this doesn't need to "own" the string just the resource it represents
            explicit SocketPathGuard(const std::string& socket_path) noexcept : path_(socket_path){
            }
            ~SocketPathGuard() noexcept{
                if(owns_path_){
                    ::unlink(path_.c_str());
                }
            }

            SocketPathGuard(const SocketPathGuard&) = delete;
            SocketPathGuard& operator=(const SocketPathGuard&) = delete;
            SocketPathGuard(SocketPathGuard&& other) noexcept{
                path_ = std::move(other.path_);
                owns_path_ = other.owns_path_;
                other.owns_path_ = false;
            }
            SocketPathGuard& operator=(SocketPathGuard&& other) noexcept{
                if(this != &other){
                    if(owns_path_){
                        ::unlink(path_.c_str());
                    }
                    path_ = std::move(other.path_);
                    owns_path_ = other.owns_path_;
                    other.owns_path_ = false;
                    other.path_.clear();
                }
                return *this;
            }

            void mark_owned() noexcept{
                owns_path_ = true;
            }

        private:
            std::string path_;
            bool owns_path_ = false;
    };
}