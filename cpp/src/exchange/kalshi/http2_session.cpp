#include "predex/exchange/kalshi/http2_session.hpp"
#include "predex/exchange/kalshi/http_types.hpp"
#include <curl/easy.h>
#include <curl/multi.h>

namespace {
    [[nodiscard]] std::uint64_t now_ns() noexcept{
        return static_cast<std::uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::steady_clock::now().time_since_epoch()
            ).count()
        );
    }
}
namespace predex::exchange::kalshi{

    Http2Session::Http2Session(AuthSigner signer, Http2SessionConfig config) : signer_(std::move(signer)), config_(std::move(config)) {
    }


    Http2Session::Http2Session(Http2Session&& other) noexcept:
        signer_(std::move(other.signer_)),
        config_(std::move(other.config_)),
        curl_multi_handle_(other.curl_multi_handle_),
        inflight_requests_(std::move(other.inflight_requests_)),
        last_error_(std::move(other.last_error_)),
        is_closed_(other.is_closed_),
        is_warmed_up_(other.is_warmed_up_),
        completed_responses_(std::move(other.completed_responses_))
    {

        other.curl_multi_handle_ = nullptr;
        other.is_closed_ = true;
        other.is_warmed_up_ = false;
    }

    Http2Session& Http2Session::operator=(Http2Session&& other) noexcept{
        if(this != &other){
            this->close();
            signer_ = std::move(other.signer_);
            config_ = std::move(other.config_);
            curl_multi_handle_ = other.curl_multi_handle_;
            inflight_requests_ = std::move(other.inflight_requests_);
            last_error_ = std::move(other.last_error_);
            completed_responses_ = std::move(other.completed_responses_);
            is_closed_ = other.is_closed_;
            is_warmed_up_ = other.is_warmed_up_;
            
            other.curl_multi_handle_ = nullptr;
            other.is_closed_ = true;
            other.is_warmed_up_ = false;
        }
        return *this;
    }




    Http2Session::~Http2Session(){
        close();
    }

    bool Http2Session::warm_up(){
        if(!is_closed_){
            return true;
        }
        curl_multi_handle_ = curl_multi_init();
        if(curl_multi_handle_ == nullptr){
            last_error_ = "Failed to initialize CURL multi handle";
            return false;
        }
        is_closed_ = false;
        is_warmed_up_ = true;
        return true;

    }

    void Http2Session::close() noexcept{
        if(curl_multi_handle_ != nullptr && !inflight_requests_.empty()){
            for(auto&[_, request] : inflight_requests_){
                if(request.curl_handle != nullptr){
                    curl_multi_remove_handle(curl_multi_handle_, request.curl_handle);
                    curl_easy_cleanup(request.curl_handle);
                    request.curl_handle = nullptr;
                }
            }
            curl_multi_cleanup(curl_multi_handle_);
            curl_multi_handle_ = nullptr;
            is_closed_ = true;
            is_warmed_up_ = false;
            inflight_requests_.clear();
        }else{
            if(curl_multi_handle_ != nullptr){
                curl_multi_cleanup(curl_multi_handle_);
                curl_multi_handle_ = nullptr;
                is_closed_ = true;
                is_warmed_up_ = false;
            }
        }

        is_closed_ = true;
        is_warmed_up_ = false;
    }

    std::string_view Http2Session::last_error() const noexcept{
        return last_error_;
    }

    bool Http2Session::has_inflight_requests() const noexcept{
        return !inflight_requests_.empty();
    }

    std::size_t Http2Session::available_capacity() const noexcept{
        if(is_closed_ || config_.max_concurrent_streams <= inflight_requests_.size()){
            return 0;
        }
        return config_.max_concurrent_streams - inflight_requests_.size();
    }

    HttpPollResult Http2Session::poll(){
        if(!completed_responses_.empty()){
            HttpResponse response = std::move(completed_responses_.front());
            completed_responses_.pop();
            return HttpPollResult{HttpRequestStatus::kCOMPLETED, std::move(response)};
        }
        if(is_closed_){
            return HttpPollResult{HttpRequestStatus::kIDLE, std::nullopt};
        }

        int still_running = 0;
        CURLMcode multi_code = curl_multi_perform(curl_multi_handle_, &still_running);
        if(multi_code != CURLM_OK){
            last_error_ = "CURL multi perform error: " + std::string{curl_multi_strerror(multi_code)};
            return HttpPollResult{has_inflight_requests() ? HttpRequestStatus::kIN_FLIGHT : HttpRequestStatus::kIDLE, std::nullopt};
        }

        int queued = 0;
        while(CURLMsg* msg = curl_multi_info_read(curl_multi_handle_, &queued)){
            if(msg->msg == CURLMSG_DONE){
                CURL* easy_handle = msg->easy_handle;
                void* request_id_pointer = nullptr;

                curl_easy_getinfo(easy_handle, CURLINFO_PRIVATE, &request_id_pointer);
                
                auto request_id = static_cast<HttpRequestId>(reinterpret_cast<std::uintptr_t>(request_id_pointer));

                auto iter = inflight_requests_.find(request_id);

                if(iter == inflight_requests_.end()){
                    last_error_ = "Received CURLMSG_DONE for unknown request_id";
                    curl_multi_remove_handle(curl_multi_handle_, easy_handle);
                    curl_easy_cleanup(easy_handle);
                    continue;
                }

                ActiveRequest& active_req = iter->second;
                long http_code = 0;
                curl_easy_getinfo(easy_handle, CURLINFO_RESPONSE_CODE, &http_code);

                long http_version = 0;
                curl_easy_getinfo(easy_handle, CURLINFO_HTTP_VERSION, &http_version);
                
                HttpResponse response;
                response.request_id = request_id;
                response.status_code = static_cast<std::uint16_t>(http_code);
                response.body = std::move(active_req.response_body);
                response.error_message = msg->data.result == CURLE_OK ? "" : curl_easy_strerror(msg->data.result);
                response.ok = (msg->data.result == CURLE_OK && http_code >= 200 && http_code < 300); //NOLINT
                response.trace = active_req.request.trace;
                response.trace.response_recv_ts_ns = now_ns();
                response.trace.negotiated_protocol = (http_version == CURL_HTTP_VERSION_2_0) ? HttpProtocol::kHTTP_2 : HttpProtocol::kHTTP_1_1;

                curl_multi_remove_handle(curl_multi_handle_, easy_handle);

                completed_responses_.push(std::move(response));
                inflight_requests_.erase(iter);
            }
            if(!completed_responses_.empty()){
                HttpResponse response = std::move(completed_responses_.front());
                completed_responses_.pop();
                return HttpPollResult{HttpRequestStatus::kCOMPLETED, std::move(response)};
            }
        }
        return HttpPollResult{has_inflight_requests() ? HttpRequestStatus::kIN_FLIGHT : HttpRequestStatus::kIDLE, std::nullopt};
    }

    HttpStartResult Http2Session::start_request(HttpRequest request){
        if(is_closed_ || curl_multi_handle_ == nullptr){
            return HttpStartResult::kCLOSED;
        }
        if(available_capacity() == 0){
            return HttpStartResult::kAT_CAPACITY;
        }
        if(request.request_id == 0){
            return HttpStartResult::kERROR;
        }
        if(inflight_requests_.contains(request.request_id)){
            return HttpStartResult::kERROR;
        }

        ActiveRequest active;
        active.request = std::move(request);
        active.url = config_.endpoint + active.request.target;
        active.body = std::move(active.request.body);
        active.error_buffer.resize(CURL_ERROR_SIZE, '\0');
        active.deadline_ns = now_ns() + config_.request_timeout.count();
        inflight_requests_.emplace(active.request.request_id, std::move(active));
        


    }







}