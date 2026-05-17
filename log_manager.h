#ifndef _LOG_MANAGER_H_
#define _LOG_MANAGER_H_ 1

#include <string>
#include <fstream>
#include <ctime>
#include <sstream>

class LogManager {
public:
    static LogManager& getInstance() {
        static LogManager instance;
        return instance;
    }
    void log(const std::string& operation, const std::string& detail) {
        if (!log_file_.is_open()) return;
        std::time_t now = std::time(nullptr);
        char time_buf[64];
        std::strftime(time_buf, sizeof(time_buf), "%Y-%m-%d %H:%M:%S", std::localtime(&now));
        log_file_ << "[" << time_buf << "] " << operation << ": " << detail << std::endl;
        log_file_.flush();
    }
    void enable() {
        if (!log_file_.is_open()) {
            log_file_.open("minisql.log", std::ios::app);
        }
    }
    void disable() {
        if (log_file_.is_open()) {
            log_file_.close();
        }
    }
    bool isEnabled() {
        return log_file_.is_open();
    }
private:
    LogManager() { enable(); }
    ~LogManager() { disable(); }
    LogManager(const LogManager&);
    LogManager& operator=(const LogManager&);
    std::ofstream log_file_;
};

#endif
