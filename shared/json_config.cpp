#include "json_config.h"

#include <cctype>
#include <fstream>
#include <iterator>

namespace {
void SkipWs(const std::string& s, size_t& i)
{
    while (i < s.size() && std::isspace(static_cast<unsigned char>(s[i])))
    {
        ++i;
    }
}

bool ParseString(const std::string& s, size_t& i, std::string& out)
{
    if (i >= s.size() || s[i] != '"')
    {
        return false;
    }
    ++i;
    out.clear();

    while (i < s.size())
    {
        char c = s[i++];
        if (c == '"')
        {
            return true;
        }
        if (c == '\\')
        {
            if (i >= s.size())
            {
                return false;
            }
            char esc = s[i++];
            switch (esc)
            {
            case '"': out.push_back('"'); break;
            case '\\': out.push_back('\\'); break;
            case '/': out.push_back('/'); break;
            case 'b': out.push_back('\b'); break;
            case 'f': out.push_back('\f'); break;
            case 'n': out.push_back('\n'); break;
            case 'r': out.push_back('\r'); break;
            case 't': out.push_back('\t'); break;
            default: return false;
            }
        }
        else
        {
            out.push_back(c);
        }
    }
    return false;
}

bool ParseUInt64(const std::string& s, size_t& i, uint64_t& out)
{
    if (i >= s.size() || !std::isdigit(static_cast<unsigned char>(s[i])))
    {
        return false;
    }

    uint64_t value = 0;
    while (i < s.size() && std::isdigit(static_cast<unsigned char>(s[i])))
    {
        value = value * 10 + static_cast<uint64_t>(s[i] - '0');
        ++i;
    }
    out = value;
    return true;
}
}

bool JsonConfig::LoadFromFile(const std::string& path, JsonConfig& out, std::string& err)
{
    std::ifstream ifs(path);
    if (!ifs.is_open())
    {
        err = "open failed: " + path;
        return false;
    }

    std::string s((std::istreambuf_iterator<char>(ifs)), std::istreambuf_iterator<char>());

    out.string_values_.clear();
    out.number_values_.clear();

    size_t i = 0;
    SkipWs(s, i);
    if (i >= s.size() || s[i] != '{')
    {
        err = "json must start with '{'";
        return false;
    }
    ++i;

    while (true)
    {
        SkipWs(s, i);
        if (i >= s.size())
        {
            err = "unexpected end of json";
            return false;
        }

        if (s[i] == '}')
        {
            ++i;
            break;
        }

        std::string key;
        if (!ParseString(s, i, key))
        {
            err = "invalid key";
            return false;
        }

        SkipWs(s, i);
        if (i >= s.size() || s[i] != ':')
        {
            err = "missing ':' after key";
            return false;
        }
        ++i;

        SkipWs(s, i);
        if (i >= s.size())
        {
            err = "unexpected end after ':'";
            return false;
        }

        if (s[i] == '"')
        {
            std::string v;
            if (!ParseString(s, i, v))
            {
                err = "invalid string value for key: " + key;
                return false;
            }
            out.string_values_[key] = v;
        }
        else
        {
            uint64_t n = 0;
            if (!ParseUInt64(s, i, n))
            {
                err = "invalid numeric value for key: " + key;
                return false;
            }
            out.number_values_[key] = n;
        }

        SkipWs(s, i);
        if (i >= s.size())
        {
            err = "unexpected end after value";
            return false;
        }

        if (s[i] == ',')
        {
            ++i;
            continue;
        }

        if (s[i] == '}')
        {
            ++i;
            break;
        }

        err = "expected ',' or '}'";
        return false;
    }

    SkipWs(s, i);
    if (i != s.size())
    {
        err = "trailing characters after json object";
        return false;
    }

    return true;
}

bool JsonConfig::GetString(const std::string& key, std::string& out) const
{
    auto it = string_values_.find(key);
    if (it == string_values_.end())
    {
        return false;
    }
    out = it->second;
    return true;
}

bool JsonConfig::GetUInt16(const std::string& key, uint16_t& out) const
{
    auto it = number_values_.find(key);
    if (it == number_values_.end() || it->second > 65535)
    {
        return false;
    }
    out = static_cast<uint16_t>(it->second);
    return true;
}

bool JsonConfig::GetSize(const std::string& key, size_t& out) const
{
    auto it = number_values_.find(key);
    if (it == number_values_.end())
    {
        return false;
    }
    out = static_cast<size_t>(it->second);
    return true;
}
