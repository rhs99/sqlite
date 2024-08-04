#ifndef QUERY_PARSER_H
#define QUERY_PARSER_H

#include <bits/stdc++.h>
#include "utils.h"
using namespace std;

class QueryParser
{
public:
    bool is_count_query = false;
    string query_table;
    vector<string> query_columns;
    string condition_column = "", condition_value = "";

    QueryParser(string query)
    {
        if (query.find("count(*)") != string::npos || query.find("COUNT(*)") != string::npos)
        {
            is_count_query = true;
            query_table = tokenize(query, " ").back();
        }
        else
        {
            if (query.find("where") != string::npos || query.find("WHERE") != string::npos)
            {
                int p = query.find("WHERE");
                if (p == string::npos)
                {
                    p = query.find("where");
                }

                string x = query.substr(0, p - 0);
                string y = query.substr(p, query.size() - p + 1);

                vector<string> tokens = tokenize(x, " ");
                bool is_query_coulmn_end = false;

                for (int i = 1; i < tokens.size(); i++)
                {
                    if (get_lowercase(tokens[i]) == "from")
                    {
                        is_query_coulmn_end = true;
                        query_table = tokens[i + 1];
                        break;
                    }

                    if (!is_query_coulmn_end)
                    {
                        if (tokens[i].back() == ',')
                        {
                            query_columns.push_back(tokens[i].substr(0, tokens[i].size() - 1));
                        }
                        else
                        {
                            query_columns.push_back(tokens[i]);
                        }
                    }
                }

                if (y.size() > 0)
                {
                    int z = y.find("=");
                    string col = y.substr(5, z - 5);
                    string val = y.substr(z + 1, y.size() - z - 1);
                    col.erase(remove(col.begin(), col.end(), ' '), col.end());
                    int k = val.find("'");
                    val = val.substr(k + 1, val.size() - k - 2);

                    condition_column = col;
                    condition_value = val;
                }
            }
            else
            {
                vector<string> tokens = tokenize(query, " ");
                bool is_query_coulmn_end = false;

                for (int i = 1; i < tokens.size(); i++)
                {
                    if (get_lowercase(tokens[i]) == "from")
                    {
                        is_query_coulmn_end = true;
                        query_table = tokens[i + 1];
                        break;
                    }

                    if (!is_query_coulmn_end)
                    {
                        if (tokens[i].back() == ',')
                        {
                            query_columns.push_back(tokens[i].substr(0, tokens[i].size() - 1));
                        }
                        else
                        {
                            query_columns.push_back(tokens[i]);
                        }
                    }
                }
            }
        }
    }

    void print_query()
    {
        cout << "Query Table: " << query_table << endl;
        cout << "Is Count Query: " << is_count_query << endl;
        cout << "Query Columns: ";
        for (auto x : query_columns)
        {
            cout << x << " ";
        }
        cout << endl;
        cout << "Condition Column: " << condition_column << endl;
        cout << "Condition Value: " << condition_value << endl;
    }
};

#endif