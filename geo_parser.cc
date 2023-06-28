#include "geo_parser.h"

#include <mysql/mysql.h>
#include <cppconn/driver.h>
#include <cppconn/exception.h>
#include <cppconn/resultset.h>
#include <cppconn/statement.h>
#include <cppconn/prepared_statement.h>


#include "base/exceptions.h"
#include "base/file_utils.h"
#include "base/geo_db.h"
#include "base/log.h"
#include "base/utils.h"
#include "base/iso2Toiso3.h"

using namespace ggAdNet;
using namespace ggAdNet::Tools;
using namespace rapidjson;

GeoParser::GeoParser() : countryId_(0), stateId_(0), cityId_(0)
{
    auto config = Utils::loadJsonFile(configFile_);
    initConfig(config);
    Log::init(config);
}

GeoParser::~GeoParser()
{
    Log::clean();
}

void
GeoParser::run()
{
    /**/
    auto begin = Utils::nowMicros();
    loadFromDb();
    logInfo("loaded from db in %f sec", (double) (Utils::nowMicros() - begin) / 1000000.0);
    /**/
    begin = Utils::nowMicros();
    loadLocations(maxmindLocationsEnFile_, true);
    logInfo("en locations loaded in %f sec", (double) (Utils::nowMicros() - begin) / 1000000.0);
    /**/
    begin = Utils::nowMicros();
    loadLocations(maxmindLocationsRuFile_);
    logInfo("ru locations loaded in %f sec", (double) (Utils::nowMicros() - begin) / 1000000.0);
    /**/
    begin = Utils::nowMicros();
    loadIPv4Blocks();
    logInfo("ipv4 loaded in %f sec", (double) (Utils::nowMicros() - begin) / 1000000.0);
    /**/
    begin = Utils::nowMicros();
    loadIPv6Blocks();
    logInfo("ipv6 loaded in %f sec", (double) (Utils::nowMicros() - begin) / 1000000.0);
    /**/
    begin = Utils::nowMicros();
    saveGeoDb();
    logInfo("geodb saved in %f sec", (double) (Utils::nowMicros() - begin) / 1000000.0);
    /**/
    begin = Utils::nowMicros();
    saveToDb();
    logInfo("saved to db in %f sec", (double) (Utils::nowMicros() - begin) / 1000000.0);    
}

void
GeoParser::initConfig(const rapidjson::Document& config)
{
    geoDbFile_ = defaultGeoDbFile_;
    /*  db  */
    const auto& db = Utils::configSection(config, "db");
    dbHost_ = Utils::configString(db, "host", "localhost");
    dbPort_ = Utils::configInt(db, "port", 3306);
    dbUser_ = Utils::configMandatoryString(db, "user");
    dbPassword_ = Utils::configMandatoryString(db, "password");
    dbDb_ = Utils::configMandatoryString(db, "db");
    /*  maxmind  */
    if (config.HasMember("maxmind")) {
        if (!config["maxmind"].IsObject()) {
            throw ConfigException("section maxmind must be an object");
        }
        const auto& mm = config["maxmind"];
        maxmindPath_ = Utils::configString(mm, "path", defaultMaxmindPath_);
        maxmindIpv4File_ = Utils::configString(mm, "ipv4_file", defaultMaxmindIpv4File_);
        maxmindIpv6File_ = Utils::configString(mm, "ipv6_file", defaultMaxmindIpv6File_);
        maxmindLocationsRuFile_ = Utils::configString(mm, "locations_ru_file", defaultMaxmindLocationsRuFile_);
        maxmindLocationsEnFile_ = Utils::configString(mm, "locations_en_file", defaultMaxmindLocationsEnFile_);
    } else {
        maxmindPath_ = defaultMaxmindPath_;
        maxmindIpv4File_ = defaultMaxmindIpv4File_;
        maxmindIpv6File_ = defaultMaxmindIpv6File_;
        maxmindLocationsRuFile_ = defaultMaxmindLocationsRuFile_;
        maxmindLocationsEnFile_ = defaultMaxmindLocationsEnFile_;
    }
    /**/
    geoDbFile_ = Utils::configString(db, "geodb_file", defaultGeoDbFile_);
}

void
GeoParser::loadFromDb()
{
    try {
        /*  connect  */
        sql::Driver *driver = get_driver_instance();
        std::string connUri = "tcp://" + dbHost_ + ":" + std::to_string(dbPort_);
        std::unique_ptr<sql::Connection> conn(driver->connect(connUri, dbUser_, dbPassword_));
        conn->setSchema(dbDb_);
        /*  load countries  */
        int count = 0;
        std::unique_ptr<sql::Statement> stmt(conn->createStatement());
        std::unique_ptr<sql::ResultSet> res(stmt->executeQuery("select id, `key`, name, name_en, weight from countries"));
        while (res->next()) {
            Country country;
            country.id = res->getUInt("id");
            country.key = res->getString("key");
            country.name = res->getString("name");
            country.nameEn = res->getString("name_en");
            country.weight = res->getUInt("weight");
            countries_[country.key] = country;
            if (country.id > countryId_) {
                countryId_ = country.id;
            }
            count++;
        }
        countryId_++;
        logInfo("loaded %d countries from db", count);
        /*  load states  */
        count = 0;
        res.reset(stmt->executeQuery("select id, country_id, `key`, name, name_en, weight from states"));
        while (res->next()) {
            State state;
            state.id = res->getUInt("id");
            state.countryId = res->getUInt("country_id");
            state.key = res->getString("key");
            state.name = res->getString("name");
            state.nameEn = res->getString("name_en");
            state.weight = res->getUInt("weight");
            states_[state.key] = state;
            if (state.id > stateId_) {
                stateId_ = state.id;
            }
            count++;
        }
        stateId_++;
        logInfo("loaded %d states from db", count);
        /*  load cities  */
        count = 0;
        res.reset(stmt->executeQuery("select id, state_id, `key`, name, name_en, weight from cities"));
        while (res->next()) {
            City city;
            city.id = res->getUInt("id");
            city.stateId = res->getUInt("state_id");
            city.key = res->getString("key");
            city.name = res->getString("name");
            city.nameEn = res->getString("name_en");
            city.weight = res->getUInt("weight");
            cities_[city.key] = city;
            if (city.id > cityId_) {
                cityId_ = city.id;
            }
            count++;
        }
        cityId_++;
        logInfo("loaded %d cities from db", count);
    } catch (sql::SQLException &e) {
        logError("db error: %s, code: %d", e.what(), e.getErrorCode());
        throw GeoParserException("db exception");
    }
}

void
GeoParser::loadLocations(const std::string& f, bool en)
{
    std::string file = maxmindPath_ + f;
    FileUtils::Mmap mmap(file);
    if (mmap.open() != FileUtils::Mmap::ReturnCode::SUCCESS) {
        logError("can't mmap file %s", file.c_str());
        throw GeoParserException("can't mmap file");
    }
    const char *p = mmap.ptr();
    if (!p) {
        logError("file %s is empty", file.c_str());
        throw GeoParserException("names file is empty");
    }
    const char *end = p + mmap.size();
    /* check header  */
    static const std::vector<std::string> fields = {"geoname_id", "locale_code", "continent_code",
        "continent_name", "country_iso_code", "country_name", "subdivision_1_iso_code", "subdivision_1_name",
        "subdivision_2_iso_code", "subdivision_2_name", "city_name", "metro_code", "time_zone", "is_in_european_union"};
    std::vector<CString> values;
    p = Utils::loadCSVLine(p, end, values);
    if (values.size() != fields.size()) {
        logError("bad file format %s", file.c_str());
        throw GeoParserException("bad file format");
    }
    for (unsigned int i = 0; i < fields.size(); i++) {
        if (values[i] != fields[i]) {
            logError("field #%u must be %s (%.*s got) in file %s%", i, fields[i].c_str(),
                values[i].size, values[i].data, file.c_str());
            throw GeoParserException("bad file format");
        }
    }
    /*  load data  */
    int line = 0;
    while ((p = Utils::loadCSVLine(p, end, values)) != end) {
        if (values.size() != fields.size()) {
            logError("fields count %d != %d in line %d in file %s", values.size(), fields.size(), line, file.c_str());
            throw GeoParserException("bad file format");
        }
        if (values[4].size == 0) {
            line++;
            continue;
        }
        Location location;
        unsigned int locationId = Utils::atoui(values[0]);
        /*  process country  */
        std::string key(values[4].data, values[4].size);
        auto icountry = countries_.find(key);
        if (icountry == countries_.end()) {
            /*  new country, add it  */
            Country country;
            country.id = countryId_++;
            country.key = key;
            country.name.assign(values[5].data, values[5].size);
            if (en) {
                country.nameEn.assign(values[5].data, values[5].size);
            }
            country.weight = country.id;
            country.store = true;
            countries_[key] = country;
            location.countryId = country.id;
            location.countryKey.assign(values[4].data, values[4].size);
        } else {
            if (!values[5].empty()) {
                if (values[5] != icountry->second.name) {
                    icountry->second.name.assign(values[5].data, values[5].size);
                    icountry->second.store = true;
                }
                if (en && values[5] != icountry->second.nameEn) {
                    icountry->second.nameEn.assign(values[5].data, values[5].size);
                    icountry->second.store = true;
                }
            }
            location.countryId = icountry->second.id;
            location.countryKey.assign(values[4].data, values[4].size);
        }
        /*  process state  */
        if (values[6].size != 0) {
            key.push_back('.');
            key.append(values[6].data, values[6].size);
            auto istate = states_.find(key);
            if (istate == states_.end()) {
                /*  new state, add it  */
                State state;
                state.id = stateId_++;
                state.countryId = location.countryId;
                state.key = key;
                if (values[7].size) {
                    state.name.assign(values[7].data, values[7].size);
                    if (en) {
                        state.nameEn.assign(values[7].data, values[7].size);
                    }
                } else {
                    state.name.assign(values[6].data, values[6].size);
                    if (en) {
                        state.name.assign(values[6].data, values[6].size);
                    }
                }
                state.weight = state.id;
                state.store = true;
                states_[key] = state;
                location.stateId = state.id;
                location.stateKey.assign(values[6].data, values[6].size);
            } else {
                if (!values[7].empty()) {
                    if (values[7] != istate->second.name) {
                        istate->second.name.assign(values[7].data, values[7].size);
                        istate->second.store = true;
                    }
                    if (en && values[7] != istate->second.nameEn) {
                        istate->second.nameEn.assign(values[7].data, values[7].size);
                        istate->second.store = true;
                    }
                }
                location.stateId = istate->second.id;
                location.stateKey.assign(values[6].data, values[6].size);
            }
            /*  process city  */
            if (values[10].size != 0) {
                /*  use geoname_id as identifier in key  */
                key.push_back('.');
                key.append(values[0].data, values[0].size);
                auto icity = cities_.find(key);
                if (icity == cities_.end()) {
                    /*  new city, add it  */
                    City city;
                    city.id = cityId_++;
                    city.stateId = location.stateId;
                    city.key = key;
                    city.name.assign(values[10].data, values[10].size);
                    if (en) {
                        city.nameEn.assign(values[10].data, values[10].size);
                        location.cityName.assign(city.nameEn, city.nameEn.size());
                    }
                    city.weight = city.id;
                    city.store = true;
                    cities_[key] = city;
                    location.cityId = city.id;
                } else {
                    if (!values[10].empty()) {
                        if (values[10] != icity->second.name) {
                            icity->second.name.assign(values[10].data, values[10].size);
                            icity->second.store = true;
                        }
                        if (en && values[10] != icity->second.nameEn) {
                            icity->second.nameEn.assign(values[10].data, values[10].size);
                            icity->second.store = true;
                            location.cityName.assign(icity->second.nameEn, icity->second.nameEn.size());
                        }
                    }
                    location.cityId = icity->second.id;
                }
            }
        }
        if (location.stateId == 703883) {
            /*  крымнаш  */
            location.countryId = 2017370;
        }
        locations_[locationId] = location;
        line++;
    }
}

void
GeoParser::loadIPv4Blocks()
{
    std::string file = maxmindPath_ + maxmindIpv4File_;
    FileUtils::Mmap mmap(file);
    if (mmap.open() != FileUtils::Mmap::ReturnCode::SUCCESS) {
        logError("can't mmap file %s", file.c_str());
        throw GeoParserException("can't mmap file");
    }
    const char *p = mmap.ptr();
    if (!p) {
        logError("file %s is empty", file.c_str());
        throw GeoParserException("names file is empty");
    }
    const char *end = p + mmap.size();
    /* check header  */
    static const std::vector<std::string> fields = {"network", "geoname_id", "registered_country_geoname_id",
        "represented_country_geoname_id", "is_anonymous_proxy", "is_satellite_provider", "postal_code",
        "latitude", "longitude", "accuracy_radius"};
    std::vector<CString> values;
    p = Utils::loadCSVLine(p, end, values);
    if (values.size() != fields.size()) {
        logError("bad file format %s", file.c_str());
        throw GeoParserException("bad file format");
    }
    for (unsigned int i = 0; i < fields.size(); i++) {
        if (values[i] != fields[i]) {
            logError("field #%u must be %s (%.*s got) in file %s", i, fields[i].c_str(),
                values[i].size, values[i].data, file.c_str());
            throw GeoParserException("bad file format");
        }
    }
    /*  load data  */
    int line = 0;
    while ((p = Utils::loadCSVLine(p, end, values)) != end) {
        if (values.size() != fields.size()) {
            logError("fields count %d != %d in line %d in file %s", values.size(), fields.size(), line, file.c_str());
            throw GeoParserException("bad file format");
        }
        const char *p = (const char *) memchr(values[0].data, '/', values[0].size);
        if (!p) {
            logWarn("bad network in line %d in file %s", line, file.c_str());
            line++;
            continue;
        }
        std::string network(values[0].data, values[0].size);
        GeoDb::IPv4 ipFrom;
        GeoDb::IPv4 ipTo;
        GeoDb::net4ToRange(network, ipFrom, ipTo);
        unsigned int locationId = Utils::atoui(values[1]);
        auto it = locations_.find(locationId);
        if (it == locations_.end()) {
            if (values[2].size) {
                locationId = Utils::atoui(values[2]);
                it = locations_.find(locationId);
                if (it == locations_.end()) {
                    line++;
                    continue;
                }
            } else {
                line++;
                continue;
            }
        }
        auto r = geodb_.add_ipsv4();
        r->set_from(ipFrom);
        r->set_to(ipTo);
        r->set_country_id(it->second.countryId);
        r->set_state_id(it->second.stateId);
        r->set_city_id(it->second.cityId);
        r->set_country_key(codeTransf.at(it->second.countryKey));
        r->set_state_key(it->second.stateKey);
        r->set_city_name(it->second.cityName);
        line++;
    }
}

void
GeoParser::loadIPv6Blocks()
{
    std::string file = maxmindPath_ + maxmindIpv6File_;
    FileUtils::Mmap mmap(file);
    if (mmap.open() != FileUtils::Mmap::ReturnCode::SUCCESS) {
        logError("can't mmap file %s", file.c_str());
        throw GeoParserException("can't mmap file");
    }
    const char *p = mmap.ptr();
    if (!p) {
        logError("file %s is empty", file.c_str());
        throw GeoParserException("names file is empty");
    }
    const char *end = p + mmap.size();
    /* check header  */
    static const std::vector<std::string> fields = {"network", "geoname_id", "registered_country_geoname_id",
        "represented_country_geoname_id", "is_anonymous_proxy", "is_satellite_provider", "postal_code",
        "latitude", "longitude", "accuracy_radius"};
    std::vector<CString> values;
    p = Utils::loadCSVLine(p, end, values);
    if (values.size() != fields.size()) {
        logError("bad file format %s", file.c_str());
        throw GeoParserException("bad file format");
    }
    for (unsigned int i = 0; i < fields.size(); i++) {
        if (values[i] != fields[i]) {
            logError("field #%u must be %s (%.*s got) in file %s", i, fields[i].c_str(),
                values[i].size, values[i].data, file.c_str());
            throw GeoParserException("bad file format");
        }
    }
    /*  load data  */
    int line = 0;
    while ((p = Utils::loadCSVLine(p, end, values)) != end) {
        if (values.size() != fields.size()) {
            logError("fields count %d != %d in line %d in file %s", values.size(), fields.size(), line, file.c_str());
            throw GeoParserException("bad file format");
        }
        const char *p = (const char *) memchr(values[0].data, '/', values[0].size);
        if (!p) {
            logWarn("bad network in line %d in file %s", line, file.c_str());
            line++;
            continue;
        }
        std::string network(values[0].data, values[0].size);
        GeoDb::IPv6 ipFrom;
        GeoDb::IPv6 ipTo;
        GeoDb::net6ToRange(network, ipFrom, ipTo);
        unsigned int locationId = Utils::atoui(values[1]);
        auto it = locations_.find(locationId);
        if (it == locations_.end()) {
            if (values[2].size) {
                locationId = Utils::atoui(values[2]);
                it = locations_.find(locationId);
                if (it == locations_.end()) {
                    line++;
                    continue;
                }
            } else {
                line++;
                continue;
            }
        }
        auto r = geodb_.add_ipsv6();
        r->set_from_hi(ipFrom.hi);
        r->set_from_lo(ipFrom.lo);
        r->set_to_hi(ipTo.hi);
        r->set_to_lo(ipTo.lo);
        r->set_country_id(it->second.countryId);
        r->set_state_id(it->second.stateId);
        r->set_city_id(it->second.cityId);
        r->set_country_key(codeTransf.at(it->second.countryKey));
        r->set_state_key(it->second.stateKey);
        r->set_city_name(it->second.cityName);
        line++;
    }
}

void
GeoParser::saveGeoDb()
{
    if (0) {
        /*  store countries  */
        for (const auto& it : countries_) {
            const auto& country = it.second;
            auto r = geodb_.add_countries();
            r->set_id(country.id);
            r->set_key(codeTransf.at(country.key));
            r->set_name(country.name);
            r->set_name_en(country.nameEn);
        }
        /*  store states  */
        for (const auto& it : states_) {
            const auto& state = it.second;
            auto r = geodb_.add_states();
            r->set_id(state.id);
            r->set_key(state.key);
            r->set_name(state.name);
            r->set_name_en(state.nameEn);
        }
        /*  store cities  */
        for (const auto& it : cities_) {
            const auto& city = it.second;
            auto r = geodb_.add_cities();
            r->set_id(city.id);
            r->set_key(city.key);
            r->set_name(city.name);
            r->set_name_en(city.nameEn);
        }
    }
    /*  serialize  */
    std::string s = geodb_.SerializeAsString();
    /*  store  */
    FILE *fd = fopen(defaultGeoDbFile_.c_str(), "wb");
    if (!fd) {
        logError("can't fopen %s for writing", defaultGeoDbFile_.c_str());
        return;
    }
    size_t rc = fwrite(s.c_str(), 1, s.size(), fd);
    if (rc != static_cast<size_t>(s.size())) {
        logError("can't write, rc: %zu, error: %s (%d)", rc, strerror(errno), errno);
        fclose(fd);
        return;
    }
    fclose(fd);
}

void
GeoParser::saveToDb()
{
    try {
        /*  connect  */
        sql::Driver *driver = get_driver_instance();
        std::string connUri = "tcp://" + dbHost_ + ":" + std::to_string(dbPort_);
        std::unique_ptr<sql::Connection> conn(driver->connect(connUri, dbUser_, dbPassword_));
        conn->setSchema(dbDb_);
        /*  save countries  */
        int created = 0;
        auto begin = Utils::nowMicros();
        std::unique_ptr<sql::PreparedStatement> stmt(conn->prepareStatement(
            "replace into countries(id, `key`, name, name_en, weight) values(?, ?, ?, ?, ?)"));
        for (const auto& it : countries_) {
            const auto& country = it.second;
            if (!country.store) {
                continue;
            }
            stmt->setUInt(1, country.id);
            stmt->setString(2, country.key);
            stmt->setString(3, country.name);
            stmt->setString(4, country.nameEn);
            stmt->setUInt(5, country.weight);
            stmt->execute();
            created++;
        }
        logInfo("created %d countries in %f sec", created, (double) (Utils::nowMicros() - begin) / 1000000.0);
        /*  save states  */
        created = 0;
        begin = Utils::nowMicros();
        stmt.reset(conn->prepareStatement(
            "replace into states(id, country_id, `key`, name, name_en, weight) values(?, ?, ?, ?, ?, ?)"));
        for (const auto& it : states_) {
            const auto& state = it.second;
            if (!state.store) {
                continue;
            }
            stmt->setUInt(1, state.id);
            stmt->setUInt(2, state.countryId);
            stmt->setString(3, state.key);
            stmt->setString(4, state.name);
            stmt->setString(5, state.nameEn);
            stmt->setUInt(6, state.weight);
            stmt->execute();
            created++;
        }
        logInfo("created %d states in %f sec", created, (double) (Utils::nowMicros() - begin) / 1000000.0);
        /*  save cities  */
        created = 0;
        begin = Utils::nowMicros();
        stmt.reset(conn->prepareStatement(
            "replace into cities(id, state_id, `key`, name, name_en, weight) values(?, ?, ?, ?, ?, ?)"));
        for (const auto& it : cities_) {
            const auto& city = it.second;
            if (!city.store) {
                continue;
            }
            stmt->setUInt(1, city.id);
            stmt->setUInt(2, city.stateId);
            stmt->setString(3, city.key);
            stmt->setString(4, city.name);
            stmt->setString(5, city.nameEn);
            stmt->setUInt(6, city.weight);
            stmt->execute();
            created++;
        }
        logInfo("created %d cities in %f sec", created, (double) (Utils::nowMicros() - begin) / 1000000.0);
    } catch (sql::SQLException &e) {
        logError("db error: %s, code: %d", e.what(), e.getErrorCode());
        throw GeoParserException("db exception");
    }
}

