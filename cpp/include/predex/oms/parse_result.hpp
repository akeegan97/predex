#pragma once

#include <optional>
#include <utility>

namespace trading::oms {

enum class ParseError : unsigned char {
    kNone = 0,
    kInvalidJson,
    kMissingField,
    kInvalidField,
    kUnsupportedMessageType,
};

template <typename T> class ParseResult {
  public:
    static ParseResult success(T value) {
        ParseResult out;
        out.value_ = std::move(value);
        out.error_ = ParseError::kNone;
        return out;
    }

    static ParseResult failure(ParseError error) {
        ParseResult out;
        out.error_ = error;
        return out;
    }

    [[nodiscard]] bool ok() const { return value_.has_value(); }
    [[nodiscard]] const std::optional<T>& value() const { return value_; }
    [[nodiscard]] ParseError error() const { return error_; }

  private:
    std::optional<T> value_;
    ParseError error_{ParseError::kNone};
};

} // namespace trading::oms
