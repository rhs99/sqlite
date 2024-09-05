DB_PATH="sample.db"

g++ src/server.cpp -o out


echo "Printing DB info:"
./out $DB_PATH .dbinfo

echo -e "\nPrinting DB table names:"
./out $DB_PATH .tables

echo -e "\nExecution SELECT COUNT(*) FROM apples"
./out $DB_PATH "SELECT COUNT(*) FROM apples"

echo -e "\nExecution SELECT COUNT(*) FROM apples:"
./out $DB_PATH "SELECT COUNT(*) FROM apples"

echo -e "\nExecution SELECT name, color FROM apples:"
./out $DB_PATH "SELECT name, color FROM apples"

echo -e "\nExecution SELECT name, color FROM apples WHERE color = Red:"
./out $DB_PATH "SELECT name, color FROM apples WHERE color = 'Red'"

rm out