#ifndef UTILS_H
#define UTILS_H

#include <bits/stdc++.h>
using namespace std;

pair<uint64_t, int> parse_varint(ifstream &stream)
{
    char c;
    stream.read(&c, 1);
    unsigned char d = static_cast<unsigned char>(c);
    uint64_t ret = 0;
    int length = 1;
    while ((d >> 7 & 1) && length < 9)
    {
        ret = ret << 7 | d & (1 << 7) - 1;
        stream.read(&c, 1);
        d = static_cast<unsigned char>(c);
        length++;
    }
    ret = ret << 7 | d;

    return make_pair(ret, length);
}

uint64_t big_endian(ifstream &stream, int length)
{
    char c;
    int len = 0;
    uint64_t ret = 0;

    while (len < length)
    {
        stream.read(&c, 1);
        unsigned char d = static_cast<unsigned char>(c);
        ret = ret << 8 | d;
        len++;
    }
    return ret;
}

std::string parse_column_value(ifstream &stream, int serial_type)
{
    if (serial_type == 0)
    {
        return "";
    }
    else if (serial_type <= 4)
    {
        uint32_t x = big_endian(stream, serial_type);
        return to_string(x);
    }
    else if (serial_type == 5)
    {
        return to_string(big_endian(stream, 6));
    }
    else if (serial_type == 6)
    {
        return to_string(big_endian(stream, 8));
    }
    else if (serial_type == 8)
    {
        return to_string(0);
    }
    else if (serial_type == 9)
    {
        return to_string(1);
    }
    else if (serial_type >= 13 && serial_type % 2 == 1)
    {
        int length = (serial_type - 13) / 2;
        char buff[length];
        stream.read(buff, length);
        std::string str(buff, buff + sizeof buff / sizeof buff[0]);
        return str;
    }
    else
    {
        cout << "Not implemented: " << serial_type << endl;
    }

    return "";
}

std::vector<std::string> parse_record(ifstream &stream)
{
    auto header = parse_varint(stream);
    int bytes_in_columns = header.first - header.second;

    std::vector<uint64_t> serial_types;

    while (bytes_in_columns > 0)
    {
        auto column_serial_type = parse_varint(stream);
        serial_types.push_back(column_serial_type.first);
        bytes_in_columns -= column_serial_type.second;
    }

    std::vector<std::string> values;
    for (int i = 0; i < serial_types.size(); i++)
    {
        values.push_back(parse_column_value(stream, serial_types[i]));
    }

    return values;
}

vector<string> tokenize(string s, string delimiter)
{
    size_t pos = 0;
    string token;
    vector<string> tokens;

    while ((pos = s.find(delimiter)) != string::npos)
    {
        token = s.substr(0, pos);
        tokens.push_back(token);
        s.erase(0, pos + delimiter.length());
    }
    if (s.size() > 0)
    {
        tokens.push_back(s);
    }

    return tokens;
}

vector<string> get_columns_from_sql(string sql)
{
    int st_pos = sql.find('(');
    int en_pos = sql.find(')');

    vector<string> columns;

    vector<string> tokens = tokenize(sql.substr(st_pos + 1, en_pos - st_pos - 1), ",");
    for (auto token : tokens)
    {
        vector<string> curr_column = tokenize(token, " ");
        for (string x : curr_column)
        {
            x.erase(remove(x.begin(), x.end(), ' '), x.end());
            x.erase(remove(x.begin(), x.end(), '\n'), x.end());
            x.erase(remove(x.begin(), x.end(), '\t'), x.end());
            if (x.size() > 0)
            {
                columns.push_back(x);
                break;
            }
        }
    }

    return columns;
}

string get_lowercase(string data)
{
    transform(data.begin(), data.end(), data.begin(), [](unsigned char c)
              { return std::tolower(c); });
    return data;
}

#endif