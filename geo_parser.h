#pragma once

#include <map>
#include <memory>
#include <string>
#include <unordered_map>

#include "rapidjson/document.h"

#include "base/file_utils.h"
#include "protobuf/geo.pb.h"

namespace ggAdNet {
namespace Tools {

class GeoParserException: public std::runtime_error
{
public:
    explicit GeoParserException(const std::string& what) : std::runtime_error(what) {};
    explicit GeoParserException(const char *what) : std::runtime_error(what) {};
};

class GeoParser
{
public:

    GeoParser();
    ~GeoParser();
    void run();

private:

    struct GeoItem {
        unsigned int id;
        std::string key;
        std::string name;
        std::string nameEn;
        unsigned int weight;
        bool store;

        GeoItem() : id(0), weight(0), store(false) {}
    };

    struct Country : public GeoItem {
    };

    struct State : public GeoItem {
        unsigned int countryId;
    };

    struct City : public GeoItem {
        unsigned int stateId;
    };

    struct Location {
        unsigned int countryId;
        unsigned int stateId;
        unsigned int cityId;
        std::string countryKey;
        std::string stateKey;
        std::string cityName;

        Location() : countryId(0), stateId(0), cityId(0) {}
    };

    void initConfig(const rapidjson::Document& config);
    void loadFromDb();
    void loadLocations(const std::string& file, bool en = false);
    void loadIPv4Blocks();
    void loadIPv6Blocks();
    void saveGeoDb();
    void saveToDb();

    const std::string configFile_ = "geo_parser.conf";
    const std::string defaultMaxmindPath_ = "./";
    const std::string defaultMaxmindIpv4File_ = "GeoLite2-City-Blocks-IPv4.csv";
    const std::string defaultMaxmindIpv6File_ = "GeoLite2-City-Blocks-IPv6.csv";
    const std::string defaultMaxmindLocationsRuFile_ = "GeoLite2-City-Locations-ru.csv";
    const std::string defaultMaxmindLocationsEnFile_ = "GeoLite2-City-Locations-en.csv";
    const std::string defaultGeoDbFile_ = "geodb.dat";

    /*  db config  */
    std::string dbHost_;
    int dbPort_;
    std::string dbUser_;
    std::string dbPassword_;
    std::string dbDb_;
    /*  maxmind config  */
    std::string maxmindPath_;
    std::string maxmindIpv4File_;
    std::string maxmindIpv6File_;
    std::string maxmindLocationsRuFile_;
    std::string maxmindLocationsEnFile_;
    /**/
    std::string geoDbFile_;
    /**/
    unsigned int countryId_;
    unsigned int stateId_;
    unsigned int cityId_;
    std::map<std::string, Country> countries_;
    std::map<std::string, State> states_;
    std::map<std::string, City> cities_;
    std::unordered_map<unsigned int, Location> locations_;
    protobuf::Geo geodb_;
};

} // end of Tools namespace
} // end of ggAdNet namespace
