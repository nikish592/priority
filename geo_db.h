#pragma once

#include <arpa/inet.h>
#include <atomic>
#include <condition_variable>
#include <map>
#include <mutex>
#include <unordered_map>
#include <set>
#include <unordered_set>
#include <thread>

#include "rapidjson/document.h"
#include "rapidjson/internal/dtoa.h"
#include "cstring.h"

namespace ggAdNet {

class GeoDbException: public std::runtime_error
{
public:
    explicit GeoDbException(const std::string& what) : std::runtime_error(what) {};
    explicit GeoDbException(const char *what) : std::runtime_error(what) {};
};

class GeoDb
{
public:

    typedef uint32_t IPv4;

    struct IPv6 {
        uint64_t hi;
        uint64_t lo;

        IPv6() : hi(0), lo(0) {}
        IPv6(uint64_t hi, uint64_t lo) : hi(hi), lo(lo) {}
        IPv6(const IPv6& rhs) = default;
        inline IPv6& operator = (const IPv6& rhs) = default;

        inline bool operator == (const IPv6& rhs) const { return hi == rhs.hi && lo == rhs.lo; }
        inline bool operator != (const IPv6& rhs) const { return !(*this == rhs); }
        inline bool operator < (const IPv6& rhs) const { return hi < rhs.hi || (hi == rhs.hi && lo < rhs.lo); }
        inline bool operator <= (const IPv6& rhs) const { return *this == rhs || *this < rhs; }
        inline bool operator > (const IPv6& rhs) const { return hi > rhs.hi || (hi == rhs.hi && lo > rhs.lo); }
        inline bool operator >= (const IPv6& rhs) const { return *this == rhs || *this > rhs; }

        [[nodiscard]] std::string toString() const {
            char buf[40];
            snprintf(buf, 40, "%04x:%04x:%04x:%04x:%04x:%04x:%04x:%04x",
                (unsigned int) ((hi & 0xffff000000000000ULL) >> 48),
                (unsigned int) ((hi & 0x0000ffff00000000ULL) >> 32),
                (unsigned int) ((hi & 0x00000000ffff0000ULL) >> 16),
                (unsigned int) ((hi & 0x000000000000ffffULL)),
                (unsigned int) ((lo & 0xffff000000000000ULL) >> 48),
                (unsigned int) ((lo & 0x0000ffff00000000ULL) >> 32),
                (unsigned int) ((lo & 0x00000000ffff0000ULL) >> 16),
                (unsigned int) ((lo & 0x000000000000ffffULL))
            );
            return buf;
        }
    };

    struct Element {
        unsigned int countryId;
        unsigned int stateId;
        unsigned int cityId;
        CString countryKey;
        CString stateKey;
        CString cityName;

        Element() : countryId(0), stateId(0), cityId(0) {}

        Element(unsigned int countryId, unsigned int stateId, unsigned int cityId, const std::string& country, const std::string& state, const std::string& city)
            : countryId(countryId), stateId(stateId), cityId(cityId), countryKey(country), stateKey(state), cityName(city) {}

        void clear() {
            countryId = 0;
            stateId = 0;
            cityId = 0;
            countryKey.clear();
            stateKey.clear();
            cityName.clear();
        }
    };

    static void init(const rapidjson::Document& config);
    static void stop();

    static bool checkIpv4(const char *p) {
        struct in_addr in{};
        return inet_pton(AF_INET, p, &in) == 1;
    }

    static bool checkIpv4(const CString& s) {
        if (s.size > INET6_ADDRSTRLEN) {
            return false;
        }
        char buf[INET6_ADDRSTRLEN + 1];
        memcpy(buf, s.data, s.size);
        buf[s.size] = '\0';
        return checkIpv4(buf);
    }

    static bool checkIpv6(const char *p) {
        struct in6_addr in{};
        return inet_pton(AF_INET6, p, &in) == 1;
    }

    static bool checkIpv6(const CString& s) {
        if (s.size > INET6_ADDRSTRLEN) {
            return false;
        }
        char buf[INET6_ADDRSTRLEN + 1];
        memcpy(buf, s.data, s.size);
        buf[s.size] = '\0';
        return checkIpv6(buf);
    }

    static IPv4 ipv4FromString(const char *p, int size);
    static IPv4 ipv4FromString(const std::string& ipStr) {
        return GeoDb::ipv4FromString(ipStr.c_str(), static_cast<int>(ipStr.length()));
    }

    static std::string ipv4ToString(IPv4 ip) {
        char buf[15];
        char *p = rapidjson::internal::u32toa((ip >> 24) & 0xff, buf);
        *p++ ='.';
        p = rapidjson::internal::u32toa((ip >> 16) & 0xff, p);
        *p++ ='.';
        p = rapidjson::internal::u32toa((ip >> 8) & 0xff, p);
        *p++ ='.';
        p = rapidjson::internal::u32toa(ip & 0xff, p);
        return {buf, static_cast<size_t>(p - buf)};
    }

    static IPv6 ipv6FromString(const std::string& ipStr);
    static IPv6 ipv6FromString(const char *p, int size) {
        std::string s(p, size);
        return GeoDb::ipv6FromString(s);
    }

    static void net4ToRange(const std::string& net, IPv4& from, IPv4& to);
    static void net6ToRange(const std::string& net, IPv6& from, IPv6& to);

    static Element getIpv4(IPv4 ip) {
        assert(instance_ != nullptr);
        return instance_->db_ ? instance_->db_->find(ip) : instance_->empty_;
    }
    static Element getIpv4(const char *p, int size) {
        assert(instance_ != nullptr);
        return instance_->db_ ? instance_->db_->find(GeoDb::ipv4FromString(p, size)) : instance_->empty_;
    }
    static Element getIpv4(const std::string& ipStr) {
        assert(instance_ != nullptr);
        return instance_->db_ ? instance_->db_->find(GeoDb::ipv4FromString(ipStr)) : instance_->empty_;
    }
    static Element getIpv4(const CString& ipStr) {
        assert(instance_ != nullptr);
        return instance_->db_ ? instance_->db_->find(GeoDb::ipv4FromString(ipStr.data, ipStr.size)) : instance_->empty_;
    }
    static Element getIpv6(const char *p, int size) {
        assert(instance_ != nullptr);
        return instance_->db_ ? instance_->db_->find(GeoDb::ipv6FromString(p, size)) : instance_->empty_;
    }
    static Element getIpv6(const std::string& ipStr) {
        assert(instance_ != nullptr);
        return instance_->db_ ? instance_->db_->find(GeoDb::ipv6FromString(ipStr)) : instance_->empty_;
    }
    static Element getIpv6(const CString& ipStr) {
        assert(instance_ != nullptr);
        return instance_->db_ ? instance_->db_->find(GeoDb::ipv6FromString(ipStr.data, ipStr.size)) : instance_->empty_;
    }
    static Element getIp(const CString& s) {
        assert(instance_ != nullptr);
        if (GeoDb::checkIpv4(s)) {
            return GeoDb::getIpv4(s);
        }
        if (GeoDb::checkIpv6(s)) {
            return GeoDb::getIpv6(s);
        }
        return instance_->empty_;
    }

#ifndef UNIT_TESTS
private:
#endif

    struct Db {

        struct IPv4Data {
            IPv4 from;
            IPv4 to;
            Element el;
        };

        struct IPv6Data {
            IPv6 from;
            IPv6 to;
            Element el;
        };

        [[nodiscard]] Element find(IPv4 ip) const {
            auto it = ipv4_.lower_bound(ip);
            if (it != ipv4_.end()) {
                auto el = it->second;
                if (el.from <= ip) {
                    return el.el;
                }
            }
            return empty_;
        }

        [[nodiscard]] Element find(IPv6 ip) const {
            auto it = ipv6_.lower_bound(ip);
            if (it != ipv6_.end()) {
                auto el = it->second;
                if (el.from <= ip) {
                    return el.el;
                }
            }
            return empty_;
        }

        void addRange(IPv4 from, IPv4 to, unsigned int countryId, unsigned int stateId, unsigned int cityId,
                const std::string& countryKey, const std::string& stateKey, const std::string& cityName) {
            auto findCo = countryKeys_.find(countryKey);
            Element el;
            if (findCo != countryKeys_.end()) {
                el.countryKey = *findCo;
            } else {
                auto p = countryKeys_.insert(countryKey);
                el.countryKey = *p.first;
            }
            auto findSt = stateKeys_.find(stateKey);
            if (findSt != stateKeys_.end()) {
                el.stateKey = *findSt;
            } else {
                auto p = stateKeys_.insert(stateKey);
                el.stateKey = *p.first;
            }
            auto findCi = cityNames_.find(cityName);
            if (findCi != cityNames_.end()) {
                el.cityName = *findCi;
            } else {
                auto p = cityNames_.insert(cityName);
                el.cityName = *p.first;
            }
            el.countryId = countryId;
            el.stateId = stateId;
            el.cityId = cityId;
            ipv4_[to] = {from, to, el};
        }

        void addRange(IPv6 from, IPv6 to, unsigned int countryId, unsigned int stateId, unsigned int cityId,
                const std::string& countryKey, const std::string& stateKey, const std::string& cityName) {
            auto findCo = countryKeys_.find(countryKey);
            Element el;
            if (findCo != countryKeys_.end()) {
                el.countryKey = *findCo;
            } else {
                auto p = countryKeys_.insert(countryKey);
                el.countryKey = *p.first;
            }
            auto findSt = stateKeys_.find(stateKey);
            if (findSt != stateKeys_.end()) {
                el.stateKey = *findSt;
            } else {
                auto p = stateKeys_.insert(stateKey);
                el.stateKey = *p.first;
            }
            auto findCi = cityNames_.find(cityName);
            if (findCi != cityNames_.end()) {
                el.cityName = *findCi;
            } else {
                auto p = cityNames_.insert(cityName);
                el.cityName = *p.first;
            }
            el.countryId = countryId;
            el.stateId = stateId;
            el.cityId = cityId;
            ipv6_[to] = {from, to, el};
        }

    private:
        Element empty_;
        std::map<IPv4, IPv4Data> ipv4_;
        std::map<IPv6, IPv6Data> ipv6_;
        std::unordered_set<std::string> stateKeys_;
        std::unordered_set<std::string> cityNames_;
        std::set<std::string> countryKeys_;
    };


    explicit GeoDb(const rapidjson::Document& config);
    GeoDb(const GeoDb&);
    GeoDb& operator=(const GeoDb&);
    ~GeoDb();
    void initConfig(const rapidjson::Document& config);
    [[nodiscard]] std::shared_ptr<Db> loadDb() const;
    void watcherThreadLoop();

    const std::string defaultGeodbFile_ = "geodb.dat";
    const double defaultCheckForUpdateTimeout_ = 5.0;

    /*  config  */
    std::string geodbFile_;
    double checkForUpdateTimeout_{defaultCheckForUpdateTimeout_};
    bool dontLoadDb_{false};
    /**/
    std::mutex watcherLock_;
    std::condition_variable watcherCond_;
    std::atomic<bool> doShutdown_;
    std::unique_ptr<std::thread> watcherThread_;
    Element empty_;
    /**/
    static GeoDb *instance_;
    std::shared_ptr<Db> db_;
};

} // end of ggAdNet namespace
