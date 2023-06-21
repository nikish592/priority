#include "geo_db.h"

#include <arpa/inet.h>

#include <chrono>
#include <memory>

#include "cstring.h"
#include "exceptions.h"
#include "file_utils.h"
#include "utils.h"

#include "protobuf/geo.pb.h"

using namespace ggAdNet;

GeoDb *GeoDb::instance_ = nullptr;

GeoDb::GeoDb(const rapidjson::Document& config)
    : db_(nullptr)
{
    initConfig(config);
    if (!dontLoadDb_) {
        db_ = loadDb();
        if (!db_) {
            throw GeoDbException("can't load db");
        }
    }
    doShutdown_.store(false);
    watcherThread_ = std::make_unique<std::thread>([this] {
        watcherThreadLoop();
    });
}

GeoDb::~GeoDb()
{
    if (watcherThread_) {
        doShutdown_.store(true);
        watcherCond_.notify_all();
        watcherThread_->join();
        watcherThread_.reset();
    }
}

void
GeoDb::init(const rapidjson::Document& config)
{
    if (instance_ != nullptr) {
        return;
    }
    instance_ = new GeoDb(config);
}

void
GeoDb::stop()
{
    if (instance_ != nullptr) {
        delete instance_;
        instance_ = nullptr;
    }
}

GeoDb::IPv4
GeoDb::ipv4FromString(const char *p, int size)
{
    IPv4 ip = 0;
    IPv4 octet = 0;
    const char *end = p + size;
    for (; p < end; p++) {
        if (*p >= '0' && *p <= '9') {
            octet = octet * 10 + (*p - '0');
        } else if (*p == '.') {
            ip = (ip << 8) | octet;
            octet = 0;
        } else {
            return 0;
        }
    }
    ip = (ip << 8) | octet;
    return ip;
}

GeoDb::IPv6
GeoDb::ipv6FromString(const std::string& ipStr)
{
    IPv6 ip;
    struct in6_addr in{};
    if (inet_pton(AF_INET6, ipStr.c_str(), &in) != 1) {
        return ip;
    }
    ip.hi = in.s6_addr[0];
    ip.hi = (ip.hi << 8) | in.s6_addr[1];
    ip.hi = (ip.hi << 8) | in.s6_addr[2];
    ip.hi = (ip.hi << 8) | in.s6_addr[3];
    ip.hi = (ip.hi << 8) | in.s6_addr[4];
    ip.hi = (ip.hi << 8) | in.s6_addr[5];
    ip.hi = (ip.hi << 8) | in.s6_addr[6];
    ip.hi = (ip.hi << 8) | in.s6_addr[7];
    ip.lo = in.s6_addr[8];
    ip.lo = (ip.lo << 8) | in.s6_addr[9];
    ip.lo = (ip.lo << 8) | in.s6_addr[10];
    ip.lo = (ip.lo << 8) | in.s6_addr[11];
    ip.lo = (ip.lo << 8) | in.s6_addr[12];
    ip.lo = (ip.lo << 8) | in.s6_addr[13];
    ip.lo = (ip.lo << 8) | in.s6_addr[14];
    ip.lo = (ip.lo << 8) | in.s6_addr[15];
    return ip;
}

void
GeoDb::net4ToRange(const std::string& net, IPv4& from, IPv4& to)
{
    const char *p = (const char *) memchr(net.c_str(), '/', net.length());
    if (!p) {
        from = to = GeoDb::ipv4FromString(net);
        return;
    }
    IPv4 ip = GeoDb::ipv4FromString(net.c_str(), static_cast<int>(p - net.c_str()));
    int m = Utils::atoi(p + 1, static_cast<int>(net.length() - (p - net.c_str() + 1)));
    uint32_t mask = 0xffffffff << (32 - m);
    from = ip & mask;
    to = ip | (~mask);
}

void
GeoDb::net6ToRange(const std::string& net, IPv6& from, IPv6& to)
{
    const char *p = (const char *) memchr(net.c_str(), '/', net.length());
    if (!p) {
        from = to = GeoDb::ipv6FromString(net);
        return;
    }
    IPv6 ip = GeoDb::ipv6FromString(net.c_str(), static_cast<int>(p - net.c_str()));
    int m = Utils::atoi(p + 1, static_cast<int>(net.length() - (p - net.c_str() + 1)));
    uint64_t hi;
    uint64_t lo;
    if (m > 64) {
        hi = 0xffffffffffffffffULL;
        lo = 0xffffffffffffffffULL << (128 - m);
    } else {
        hi = 0xffffffffffffffffULL << (64 - m);
        lo = 0;
    }
    from.hi = ip.hi & hi;
    from.lo = ip.lo & lo;
    to.hi = ip.hi | (~hi);
    to.lo = ip.lo | (~lo);
}

void
GeoDb::initConfig(const rapidjson::Document& config)
{
    geodbFile_ = defaultGeodbFile_;
    checkForUpdateTimeout_= defaultCheckForUpdateTimeout_;
    dontLoadDb_ = false;
    /*  parse  */
    if (config.HasMember("geodb")) {
        const auto& geodb = config["geodb"];
        if (geodb.HasMember("check_for_update_timeout")) {
            if (!geodb["check_for_update_timeout"].IsDouble()) {
                throw ConfigException("geodb.check_for_update_timeout must be a double");
            }
            checkForUpdateTimeout_ = geodb["check_for_update_timeout"].GetDouble();
            if (checkForUpdateTimeout_ < 2.0) {
                throw ConfigException("geodb.check_for_update_timeout can't be less than 2.0");
            }
        }
        if (geodb.HasMember("file")) {
            if (!geodb["file"].IsString()) {
                throw ConfigException("geodb.file must be a string");
            }
            geodbFile_ = geodb["file"].GetString();
        }
        if (geodb.HasMember("dont_load")) {
            if (!geodb["dont_load"].IsBool()) {
                throw ConfigException("geodb.dont_load must be a boolean");
            }
            dontLoadDb_ = geodb["dont_load"].GetBool();
        }
    }
}

std::shared_ptr<GeoDb::Db>
GeoDb::loadDb() const
{
    auto begin = Utils::nowMicros();
    FileUtils::Mmap mmap(geodbFile_);
    if (mmap.open() != FileUtils::Mmap::ReturnCode::SUCCESS) {
        logError("can't mmap file %s", geodbFile_.c_str());
        throw GeoDbException("can't mmap file");
    }
    const char *p = mmap.ptr();
    if (!p) {
        logError("file %s is empty", geodbFile_.c_str());
        throw GeoDbException("geodb file is empty");
    }
    protobuf::Geo geo;
    if (!geo.ParseFromArray(p, static_cast<int>(mmap.size()))) {
        logError("can't parse geodb file %s", geodbFile_.c_str());
        throw GeoDbException("can't parse geodb file");
    }
    /**/
    auto db = std::make_shared<Db>();
    for (int i = 0; i < geo.ipsv4_size(); i++) {
        const auto& e = geo.ipsv4(i);
        db->addRange(e.from(), e.to(), e.country_id(), e.state_id(), e.city_id(), CString(e.country_key()), CString(e.state_key()), CString(e.city_name()));
    }
    for (int i = 0; i < geo.ipsv6_size(); i++) {
        const auto& e = geo.ipsv6(i);
        IPv6 from(e.from_hi(), e.from_lo());
        IPv6 to(e.to_hi(), e.to_lo());
        db->addRange(from, to, e.country_id(), e.state_id(), e.city_id(), CString(e.country_key()), CString(e.state_key()), CString(e.city_name()));
    }
    logInfo("geodb loaded in %f sec", (double) (Utils::nowMicros() - begin) / 1000000.0);
    return db;
}

namespace {

std::chrono::time_point<std::chrono::system_clock>
timeToChrono(uint64_t tm)
{
    auto sec = static_cast<time_t>(tm / 1000000);
    auto usec = tm - sec * 1000000;
    return std::chrono::system_clock::from_time_t(sec)
        + std::chrono::microseconds(usec);
}

}

void
GeoDb::watcherThreadLoop()
{
    enum {
        s_none,
        s_started
    } state;
    state = s_none;
    time_t dbLastModified = FileUtils::lastModified(geodbFile_);
    auto nextCheck = Utils::nowMicros() + static_cast<uint64_t>(defaultCheckForUpdateTimeout_ * 1000000.0);
    /**/
    for (;;) {
        std::unique_lock<std::mutex> lock(watcherLock_);
        while (!doShutdown_.load() && Utils::nowMicros() < nextCheck) {
            watcherCond_.wait_until(lock, timeToChrono(nextCheck));
        }
        if (doShutdown_) {
            break;
        }
        switch (state) {
            case s_none:
                {
                    time_t modified = FileUtils::lastModified(geodbFile_);
                    if (modified > dbLastModified) {
                        state = s_started;
                        dbLastModified = modified;
                    }
                }
                break;
            case s_started:
                {
                    time_t modified = FileUtils::lastModified(geodbFile_);
                    if (modified == dbLastModified) {
                        auto db = loadDb();
                        if (db != nullptr) {
                            db_ = db;
                        }
                        state = s_none;
                    }
                    dbLastModified = modified;
                }
                break;
        }
        while (nextCheck < Utils::nowMicros()) {
            nextCheck += static_cast<uint64_t>(defaultCheckForUpdateTimeout_ * 1000000.0);
        }
    }
}

