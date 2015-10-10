O2DB
=====
[O2DB][2] is an Open Source PHP Database Library (PDO Class Wrapper). O2DB a PDO Class wrapper that equipped with SQL Query Builder is quite powerful.
O2DB also the future will be equipped with SQL Forge, Schema Builder and Utilities to be quite powerful as well.
Another amazing product from [PT. Lingkar Kreasi (Circle Creative)][1], released under MIT License.
[O2DB][2] is build for working more powerfull with O2System Framework, but also can be used for integrated with others as standalone version with limited features.

Currently Supported Driver
--------
- MySQL
- SQLite
For now, O2DB only supports two types of databases above. 
We sincerely hope that the developers who use other types of databases, such as Postgre, Microsoft SQL, Oracle, 4D and others, can participate to complete O2DB.

Installation
------------
The best way to install O2File is to use [Composer][8]
```
composer require o2system/o2db:'dev-master'
```

Usage
-----
```
$o2db = new \O2System\DB(array(
  'default' => array(
    'driver'              => 'mysql',
    'dsn'                 => '',
    'hostname'            => 'localhost',
    'port'                => 3306,
    'username'            => 'root',
    'password'            => 'mysql',
    'database'            => 'database_name',
    'charset'             => 'utf8',
    'collate'             => 'utf8_general_ci',
    'prefix'              => '',
    'strict_on'           => FALSE,
    'encrypt'             => FALSE,
    'compress'            => FALSE,
    'buffered'            => FALSE,
    'persistent'          => TRUE,
    'transaction_enabled' => TRUE,
    'debug_enabled'       => TRUE,
    'options'             => array(
        PDO::ATTR_CASE              => PDO::CASE_NATURAL,
        PDO::ATTR_ERRMODE           => PDO::ERRMODE_EXCEPTION,
        PDO::ATTR_ORACLE_NULLS      => PDO::NULL_NATURAL,
        PDO::ATTR_STRINGIFY_FETCHES => FALSE,
        PDO::ATTR_EMULATE_PREPARES  => FALSE
    )
  )
));

// Create DB Resource
$DB = $o2db->connect(); // automaticaly load config default

// Using DSN for MySQL
$DB = $o2db->connect('mysql://username:password@127.0.0.1/database_name');

// Using DSN for SQLite
$DB = $o2db->connect('sqlite://username:password/c:\xampp\htdocs\o2db\database\data.db');

// Create a query
$query = $DB->get_where('table_name', ['record_status' => 1]);

if($query->num_rows() > 0)
{
  foreach($query->result() as $row)
  {
    // do something here for manipulation the row result
  }
}

// Fething into object
$object = new YourObjectClassName();

if($query->num_rows() > 0)
{
  // the row become your object contains with the row result
  foreach($query->result($object) as $row)
  {
    // do something here for manipulation the row result
  }
}

// Fething into class
if($query->num_rows() > 0)
{
  // the row become your class object contains with the row result
  foreach($query->result('YourClassName') as $row)
  {
    // do something here for manipulation the row result
  }
}

// Row result as array
if($query->num_rows() > 0)
{
  // the row become your object contains with the row result
  foreach($query->result('array') as $row)
  {
    // do something here for manipulation the row result
  }
}

// Chaining Query Building
$query = $DB->select('
            tablename.name AS people_name
         ')->from('tablename')->where('record_status', 1)->order_by('record_ordering', 'ASC')->limit(10)->get();
```
If you familiar with CodeIgniter Active Record, O2DB has almost the same syntax.
More details at the Wiki. (Coming Soon)

Ideas and Suggestions
---------------------
Please kindly mail us at [developer@circle-creative.com][5] or [steeven@circle-creative.com][6].

Bugs and Issues
---------------
Please kindly submit your issues at Github so we can track all the issues along development.

System Requirements
-------------------
- PHP 5.4+
- Composer
- PDO

Credits
-------
* Founder and Lead Projects: [Steeven Andrian Salim (steevenz.com)][4]

[1]: http://www.circle-creative.com
[2]: http://www.circle-creative.com/products/o2db
[4]: http://www.steevenz.com
[5]: mailto:developer@circle-creative.com
[6]: mailto:steeven@circle-creative.com
[7]: https://packagist.org/packages/o2system/o2db
[8]: https://getcomposer.org
