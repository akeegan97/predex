#include "predex/app.hpp"

#include "../common/config.hpp"

#include <atomic>
#include <chrono>
#include <csignal>
#include <iostream>
#include <string_view>
#include <thread>

namespace {

std::atomic<bool> g_shutdown_requested{false};

constexpr int kExitSuccess = 0;
constexpr int kExitArgsFailure = 2;
constexpr int kExitConfigFailure = 3;
constexpr int kExitStartupFailure = 4;
constexpr int kExitRuntimeFailure = 5;
constexpr std::int64_t kDefaultSleepMs = 100;

std::string mask_secret(std::string_view value) {
    if (value.empty()) {
        return "<empty>";
    }
    if (value.size() <= 8) {
        return std::string(value.size(), '*');
    }
    return std::string{value.substr(0, 4)} + "..." +
        std::string{value.substr(value.size() - 4)};
}

std::string describe_pem_source(std::string_view value) {
    if (value.empty()) {
        return "empty";
    }
    if (value.find("-----BEGIN") != std::string_view::npos) {
        return "inline_pem";
    }
    return "file_path";
}

void handle_shutdown_signal(int signal) {
    if (signal == SIGINT || signal == SIGTERM) {
        g_shutdown_requested.store(true, std::memory_order_relaxed);
    }
}

} // namespace

// NOLINTNEXTLINE(bugprone-exception-escape)
int main(int argc, char** argv) {
    const auto config_path = predex::apps::resolve_config_path(argc, argv);
    if (!config_path.has_value()) {
        std::cerr << "Missing trader config path. Use --config <path> or set TRADING_CONFIG_PATH.\n";
        return kExitArgsFailure;
    }

    std::string config_error;
    auto app_config = predex::apps::load_app_config(*config_path, config_error);
    if (!app_config.has_value()) {
        std::cerr << "Invalid trader config: " << config_error << '\n';
        return kExitConfigFailure;
    }

    std::cout << "Trader credential diagnostic"
              << " | ws_endpoint=" << app_config->kalshi_ws.endpoint
              << " | key_id=" << mask_secret(app_config->kalshi_ws.key_id)
              << " | private_key_source="
              << describe_pem_source(app_config->kalshi_ws.private_key_pem)
              << " | has_private_key="
              << (!app_config->kalshi_ws.private_key_pem.empty() ? "true" : "false")
              << '\n';

    std::signal(SIGINT, handle_shutdown_signal);
    std::signal(SIGTERM, handle_shutdown_signal);

    predex::App app{std::move(*app_config)};
    if (!app.start()) {
        std::cerr << "Failed to start trader app: " << app.last_error() << '\n';
        return kExitStartupFailure;
    }

    std::atomic<bool> app_finished{false};
    std::jthread app_thread([&app, &app_finished](const std::stop_token&) {
        app.run();
        app_finished.store(true, std::memory_order_release);
    });

    while (!g_shutdown_requested.load(std::memory_order_relaxed) &&
           !app_finished.load(std::memory_order_acquire)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(kDefaultSleepMs));
    }

    if (g_shutdown_requested.load(std::memory_order_relaxed)) {
        app.stop();
    }

    if (app_thread.joinable()) {
        app_thread.join();
    }

    const auto final_error = app.last_error();
    if (!final_error.empty() && !g_shutdown_requested.load(std::memory_order_relaxed)) {
        std::cerr << "Trader app exited with error: " << final_error << '\n';
        return kExitRuntimeFailure;
    }

    return kExitSuccess;
}
