#include <bits/stdc++.h>
#include "query_parser.h"
#include "utils.h"

using namespace std;

const int DB_HEADER_SIZE = 100;
const int LEAF_PAGE_HEADER_SIZE = 8;
const int INTERIOR_PAGE_HEADER_SIZE = 12;

class MasterTableRow
{
public:
    string type;
    string name;
    int root_page;
    string sql;

    MasterTableRow(string type, string name, int root_page, string sql)
    {
        this->type = type;
        this->name = name;
        this->root_page = root_page;
        this->sql = sql;
    }
};

class DB
{
public:
    ifstream stream;
    unsigned short page_size;
    vector<MasterTableRow> master_table_rows;

    DB(string db_file_path)
    {
        this->stream = ifstream(db_file_path, ios::binary);
        this->stream.seekg(16);
        this->page_size = big_endian(this->stream, 2);
        this->set_master_table_rows();
    }

    void set_master_table_rows()
    {
        this->stream.seekg(DB_HEADER_SIZE + 3);
        unsigned short cell_count = big_endian(this->stream, 2);

        for (int i = 0; i < cell_count; i++)
        {
            this->stream.seekg(DB_HEADER_SIZE + LEAF_PAGE_HEADER_SIZE + (i * 2));
            unsigned short curr_cell_location = big_endian(this->stream, 2);
            this->stream.seekg(curr_cell_location);

            parse_varint(this->stream);
            parse_varint(this->stream);

            std::vector<std::string> row = parse_record(this->stream);
            master_table_rows.push_back(MasterTableRow(row[0], row[1], stoi(row[3]), row[4]));
        }
    }

    unsigned short get_number_of_db_tables()
    {
        int cnt = 0;
        for (auto row : master_table_rows)
        {
            if (row.type == "table")
            {
                cnt++;
            }
        }
        return cnt;
    }

    void print_of_db_table_names()
    {
        for (auto row : master_table_rows)
        {
            if (row.type == "table"){
                cout << row.name << " ";
            }
        }
        cout << endl;
    }

    uint64_t get_rows_count(uint64_t page_number)
    {
        uint64_t offset = (page_number - 1) * this->page_size;
        this->stream.seekg(offset);
        unsigned short page_type = big_endian(this->stream, 1);

        uint64_t cnt = 0;

        if (page_type == 13)
        {
            this->stream.seekg(offset + 3);
            unsigned short cell_count = big_endian(this->stream, 2);
            return cell_count;
        }
        else if (page_type == 5)
        {
            this->stream.seekg(offset + 3);
            unsigned short cell_count = big_endian(this->stream, 2);

            this->stream.seekg(offset + 8);
            uint64_t right_page_number = big_endian(this->stream, 4);

            for (int i = 0; i < cell_count; i++)
            {
                this->stream.seekg(offset + INTERIOR_PAGE_HEADER_SIZE + (i * 2));
                unsigned short curr_cell_location = big_endian(this->stream, 2);
                this->stream.seekg(offset + curr_cell_location);

                uint64_t left_page_number = big_endian(this->stream, 4);
                cnt += get_rows_count(left_page_number);
            }

            cnt += get_rows_count(right_page_number);
        }

        return cnt;
    }

    void count_all_rows(string table_name)
    {
        for (auto row : master_table_rows)
        {
            if (row.name == table_name)
            {
                cout << get_rows_count(row.root_page) << endl;
                break;
            }
        }
    }

    void traverse_pages(uint64_t page_number, int table_column_cnt, vector<int> query_column_positions, int condition_column_position, string condition_value)
    {
        uint64_t offset = (page_number - 1) * this->page_size;
        this->stream.seekg(offset);
        unsigned short page_type = big_endian(this->stream, 1);

        if (page_type == 13)
        {
            this->stream.seekg(offset + 3);
            unsigned short cell_count = big_endian(this->stream, 2);

            for (int i = 0; i < cell_count; i++)
            {
                this->stream.seekg(offset + LEAF_PAGE_HEADER_SIZE + (i * 2));
                unsigned short curr_cell_location = big_endian(this->stream, 2);
                this->stream.seekg(offset + curr_cell_location);

                parse_varint(this->stream);
                uint64_t rowid = parse_varint(this->stream).first;
                vector<string> row = parse_record(this->stream);

                if (condition_column_position != -1 && row[condition_column_position] != condition_value)
                {
                    continue;
                }

                for (int j = 0; j < query_column_positions.size(); j++)
                {
                    if (j == 0)
                    {
                        string x = row[query_column_positions[j]];
                        if (x.size() == 0)
                        {
                            cout << rowid;
                        }
                        else
                        {
                            cout << x;
                        }
                    }
                    else
                    {
                        cout << "|" << row[query_column_positions[j]];
                    }
                }
                cout << endl;
            }
        }
        else if (page_type == 5)
        {
            this->stream.seekg(offset + 3);
            unsigned short cell_count = big_endian(this->stream, 2);

            this->stream.seekg(offset + 5);
            unsigned short cell_content_area = big_endian(this->stream, 2);

            this->stream.seekg(offset + 8);
            uint64_t right_page_number = big_endian(this->stream, 4);
            this->stream.seekg(offset + cell_content_area);

            stack<uint64_t> leafs;
            leafs.push(right_page_number);

            for (int i = 0; i < cell_count; i++)
            {
                uint64_t left_page_number = big_endian(this->stream, 4);
                leafs.push(left_page_number);
                parse_varint(this->stream);
            }

            while (!leafs.empty())
            {
                traverse_pages(leafs.top(), table_column_cnt, query_column_positions, condition_column_position, condition_value);
                leafs.pop();
            }
        }
    }

    void query_by_row_id(uint64_t page_number, string query_row_id, vector<int> query_column_positions, int table_column_cnt)
    {
        uint64_t offset = (page_number - 1) * this->page_size;
        this->stream.seekg(offset);
        unsigned short page_type = big_endian(this->stream, 1);

        if (page_type == 13)
        {
            this->stream.seekg(offset + 3);
            unsigned short cell_count = big_endian(this->stream, 2);

            for (int i = 0; i < cell_count; i++)
            {
                this->stream.seekg(offset + LEAF_PAGE_HEADER_SIZE + (i * 2));
                unsigned short curr_cell_location = big_endian(this->stream, 2);
                this->stream.seekg(offset + curr_cell_location);

                parse_varint(this->stream);
                uint64_t rowid = parse_varint(this->stream).first;

                if (query_row_id != to_string(rowid))
                {
                    continue;
                }

                vector<string> row = parse_record(this->stream);

                for (int j = 0; j < query_column_positions.size(); j++)
                {
                    if (j == 0)
                    {
                        string x = row[query_column_positions[j]];
                        if (x.size() == 0)
                        {
                            cout << rowid;
                        }
                        else
                        {
                            cout << x;
                        }
                    }
                    else
                    {
                        cout << "|" << row[query_column_positions[j]];
                    }
                }
                cout << endl;
                break;
            }
        }
        else if (page_type == 5)
        {
            this->stream.seekg(offset + 3);
            unsigned short cell_count = big_endian(this->stream, 2);

            this->stream.seekg(offset + 8);
            uint64_t right_page_number = big_endian(this->stream, 4);

            bool found = false;

            for (int i = 0; i < cell_count; i++)
            {
                this->stream.seekg(offset + INTERIOR_PAGE_HEADER_SIZE + (i * 2));
                unsigned short curr_cell_location = big_endian(this->stream, 2);
                this->stream.seekg(offset + curr_cell_location);

                uint64_t left_page_number = big_endian(this->stream, 4);
                uint64_t row_id = parse_varint(this->stream).first;

                if (stoull(query_row_id) <= row_id)
                {
                    found = true;
                    query_by_row_id(left_page_number, query_row_id, query_column_positions, table_column_cnt);
                    break;
                }
            }

            if (!found)
            {
                query_by_row_id(right_page_number, query_row_id, query_column_positions, table_column_cnt);
            }
        }
    }

    void get_row_ids(uint64_t page_number, string key, vector<string> &row_ids)
    {
        uint64_t offset = (page_number - 1) * this->page_size;
        this->stream.seekg(offset);
        unsigned short page_type = big_endian(this->stream, 1);

        if (page_type == 2)
        {
            this->stream.seekg(offset + 3);
            unsigned short cell_count = big_endian(this->stream, 2);

            this->stream.seekg(offset + 8);
            uint64_t right_page_number = big_endian(this->stream, 4);

            bool found = false;

            for (int i = 0; i < cell_count; i++)
            {
                this->stream.seekg(offset + INTERIOR_PAGE_HEADER_SIZE + (i * 2));
                unsigned short curr_cell_location = big_endian(this->stream, 2);
                this->stream.seekg(offset + curr_cell_location);

                uint64_t left_page_number = big_endian(this->stream, 4);

                parse_varint(this->stream);

                auto x = parse_varint(this->stream);
                int lim = x.first - x.second;
                vector<uint64_t> serial_types;

                for (int i = 0; i < lim; i++)
                {
                    serial_types.push_back(parse_varint(this->stream).first);
                }

                string curr_key = parse_column_value(this->stream, serial_types[0]);

                if (key <= curr_key)
                {
                    found = true;
                    get_row_ids(left_page_number, key, row_ids);
                    break;
                }
            }

            if (!found)
            {
                get_row_ids(right_page_number, key, row_ids);
            }
        }
        else if (page_type == 10)
        {
            this->stream.seekg(offset + 3);
            unsigned short cell_count = big_endian(this->stream, 2);

            for (int i = 0; i < cell_count; i++)
            {
                this->stream.seekg(offset + LEAF_PAGE_HEADER_SIZE + (i * 2));
                unsigned short curr_cell_location = big_endian(this->stream, 2);
                this->stream.seekg(offset + curr_cell_location);

                parse_varint(this->stream);

                auto x = parse_varint(this->stream);
                int lim = x.first - x.second;

                vector<uint64_t> serial_types;

                for (int i = 0; i < lim; i++)
                {
                    serial_types.push_back(parse_varint(this->stream).first);
                }

                string curr_key = parse_column_value(this->stream, serial_types[0]);
                string row_id = parse_column_value(this->stream, serial_types[1]);

                if (curr_key == key)
                {
                    row_ids.push_back(row_id);
                }
            }
        }
    }

    void traverse_index(string key, uint64_t page_number, vector<int> query_column_positions, int table_column_cnt)
    {
        string idx_name = "idx_companies_country";
        uint64_t idx_root_page;

        for (auto row : master_table_rows)
        {
            if (row.name == idx_name)
            {
                idx_root_page = row.root_page;
                break;
            }
        }

        vector<string> row_ids;
        get_row_ids(idx_root_page, key, row_ids);

        for (auto x : row_ids)
        {
            query_by_row_id(page_number, x, query_column_positions, table_column_cnt);
        }
    }

    void query_rows(string table, vector<string> query_columns, string condition_column, string condition_value, bool use_index = false)
    {
        for (auto row : master_table_rows)
        {
            if (row.name == table)
            {
                vector<string> table_columns = get_columns_from_sql(row.sql);
                vector<int> query_column_positions;

                for (int i = 0; i < query_columns.size(); i++)
                {
                    for (int j = 0; j < table_columns.size(); j++)
                    {
                        if (get_lowercase(query_columns[i]) == get_lowercase(table_columns[j]))
                        {
                            query_column_positions.push_back(j);
                            break;
                        }
                    }
                }

                if (use_index)
                {
                    traverse_index(condition_value, row.root_page, query_column_positions, table_columns.size());
                    return;
                }

                int condition_column_position = -1;

                if (condition_column.size() > 0)
                {
                    for (int j = 0; j < table_columns.size(); j++)
                    {
                        if (get_lowercase(condition_column) == get_lowercase(table_columns[j]))
                        {
                            condition_column_position = j;
                            break;
                        }
                    }
                }
                assert(query_column_positions.size() == query_columns.size());

                traverse_pages(row.root_page, table_columns.size(), query_column_positions, condition_column_position, condition_value);

                break;
            }
        }
    }
};

int main(int argc, char *argv[])
{
    std::cout << std::unitbuf;
    std::cerr << std::unitbuf;

    if (argc != 3)
    {
        std::cerr << "Expected two arguments" << std::endl;
        return 1;
    }

    std::string database_file_path = argv[1];
    std::string command = argv[2];

    DB db(database_file_path);

    if (command == ".dbinfo")
    {
        cout << "database page size: " << db.page_size << endl;
        cout << "number of tables: " << db.get_number_of_db_tables() << endl;

        return 0;
    }
    else if (command == ".tables")
    {
        db.print_of_db_table_names();
        return 0;
    }
    else
    {
        QueryParser qp(command);

        if (qp.is_count_query)
        {
            db.count_all_rows(qp.query_table);
        }
        else
        {
            bool use_index = false;
            if (qp.condition_column == "country")
            {
                use_index = true;
            }

            db.query_rows(qp.query_table, qp.query_columns, qp.condition_column, qp.condition_value, use_index);
        }
    }
    return 0;
}