#pragma once 
#include <unistd.h>

namespace predex::socket{
    class UniqueFD{
        public:
            explicit UniqueFD(int file_descriptor) noexcept{
                if(file_descriptor >= 0){
                    fd_ = file_descriptor;
                }
            }
            ~UniqueFD() noexcept{
                if(fd_ != -1){
                    ::close(fd_);
                    fd_ = -1;
                }
            }

            UniqueFD(const UniqueFD&) = delete;
            UniqueFD& operator=(const UniqueFD&) = delete;

            UniqueFD(UniqueFD&& other) noexcept{
                if(fd_ != -1){
                    ::close(fd_);
                }
                fd_ = other.fd_;
                other.fd_ = -1;
            }
            UniqueFD& operator=(UniqueFD&& other) noexcept{
                if(this != &other){
                    if(fd_ != -1){
                        ::close(fd_);
                    }
                    fd_ = other.fd_;
                    other.fd_ = -1;
                }
                return *this;
            }

            [[nodiscard]] int get() const noexcept{
                return fd_;
            }
            [[nodiscard]] bool is_valid() const noexcept{
                return fd_ != -1;
            }

        private:
            int fd_{-1};
    };
}