<?php
/**
 * O2DB
 *
 * An open source PDO Wrapper for PHP 5.2.4 or newer
 *
 * This content is released under the MIT License (MIT)
 *
 * Copyright (c) 2014, PT. Lingkar Kreasi (Circle Creative).
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 * @package     O2ORM
 * @author      Steeven Andrian Salim
 * @copyright   Copyright (c) 2005 - 2014, PT. Lingkar Kreasi (Circle Creative).
 * @license     http://circle-creative.com/products/o2db/license.html
 * @license     http://opensource.org/licenses/MIT  MIT License
 * @link        http://circle-creative.com
 * @since       Version 1.0
 * @filesource
 */
// ------------------------------------------------------------------------

namespace O2System\DB\Interfaces;

// ------------------------------------------------------------------------

use O2System\DB\Exception;
use O2System\DB\Factory\Query;
use O2System\DB\Factory\Result;
use O2System\Glob\Registry;

/**
 * Connection Interface Class
 *
 * @package     O2DB
 * @subpackage  Interfaces
 * @category    Interface Class
 * @author      Circle Creative Developer Team
 * @link        http://circle-creative.com/products/o2db.html
 */
abstract class Driver
{
    /**
     * Database Engine
     *
     * @access  public
     * @type    string
     */
    public $platform;

    /**
     * Database Host
     *
     * @access  public
     * @type    string
     */
    public $hostname;

    /**
     * Database Port
     *
     * @access  public
     * @type    string
     */
    public $port;

    /**
     * Database Name
     *
     * @access  public
     * @type    string
     */
    public $database;

    /**
     * Database Username
     *
     * @access  public
     * @type    string
     */
    public $username;

    /**
     * Database Password
     *
     * @access  public
     * @type    string
     */
    public $password;

    /**
     * Database Persistent Connection Flag
     *
     * @access  public
     * @type    string
     */
    public $persistent;

    /**
     * Database Charset
     *
     * @access  public
     * @type    string
     */
    public $charset;

    /**
     * Database Collation
     *
     * @access  public
     * @type    string
     */
    public $collate;

    /**
     * Database Strict On Flag
     *
     * @access  public
     * @type    string
     */
    public $strict_on = FALSE;

    /**
     * Database Compress Flag
     *
     * @access  public
     * @type    string
     */
    public $compress = FALSE;

    /**
     * Database Prefix
     *
     * @access  public
     * @type    string
     */
    public $prefix;

    /**
     * Database Use Buffered Flag++
     *
     * @access  public
     * @type    string
     */
    public $buffered = FALSE;

    /**
     * Database Use Transaction Flag
     *
     * @access  public
     * @type    string
     */
    public $transaction_enabled = FALSE;

    /**
     * Database Debug Mode
     *
     * @access  public
     * @type    string
     */
    public $debug_enabled = FALSE;

    /**
     * Default PDO Connection Options
     *
     * @access public
     * @var array
     */
    public $options = array(
        \PDO::ATTR_CASE              => \PDO::CASE_NATURAL,
        \PDO::ATTR_ERRMODE           => \PDO::ERRMODE_EXCEPTION,
        \PDO::ATTR_ORACLE_NULLS      => \PDO::NULL_NATURAL,
        \PDO::ATTR_STRINGIFY_FETCHES => FALSE,
        \PDO::ATTR_EMULATE_PREPARES  => FALSE
    );

    /**
     * List of reserved identifiers
     *
     * Identifiers that must NOT be escaped.
     *
     * @var    string[]
     */
    protected $_reserved_identifiers = array( '*' );

    /**
     * Identifier escape character
     *
     * @var    string
     */
    protected $_escape_char = '`';

    /**
     * Query Builder
     *
     * @access  protected
     * @type    \O2System\DB\Factory\Query
     */
    protected $_builder;

    /**
     * Query Active Statement
     *
     * @access  protected
     * @type    \PDOStatement
     */
    protected $_statement;

    /**
     * Driver Result Storage Registry
     *
     * @static
     * @access  protected
     * @type    array
     */
    protected static $_registry = array();

    // ------------------------------------------------------------------------

    /**
     * Class Constructor
     *
     * @param array $connection Array of connection configuration
     *
     * @access  public
     * @throws  \Exception
     */
    public function __construct( array $connection = array() )
    {
        if( ! empty( $connection ) )
        {
            foreach( $connection as $key => $value )
            {
                if( ! empty( $value ) ) $this->{$key} = $value;
            }

            $this->_create_dsn();

            try
            {
                $this->options[ \PDO::ATTR_PERSISTENT ] = $this->persistent;

                $this->pdo = new \PDO( $this->dsn, $this->username, $this->password, $this->options );

                // We can now log any exceptions on Fatal error.
                $this->pdo->setAttribute( \PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION );

                // Disable emulation of prepared statements, use REAL prepared statements instead.
                $this->pdo->setAttribute( \PDO::ATTR_EMULATE_PREPARES, TRUE );

                // Initialize Query Builder
                $this->_builder = new Query( $this );

                // Initialize Driver Registry
                static::$_registry = new Registry( array(), $this->platform . '.registry' );
            }
            catch( \PDOException $e )
            {
                throw new Exception( 'Unable to connect to database: ' . $e->getMessage(), 200 );
            }
        }
    }

    // ------------------------------------------------------------------------

    /**
     * Magic Method __call
     *
     * Route default method calling to query builder class
     *
     * @param   string $method Method name
     * @param   array  $args   Method parameters
     *
     * @final   This method can't be overwrite
     * @access  public
     * @return  mixed
     */
    final public function __call( $method, $args )
    {
        if( method_exists( $this->_builder, $method ) )
        {
            return call_user_func_array( array( $this->_builder, $method ), $args );
        }
    }

    // ------------------------------------------------------------------------

    /**
     * Magic Method __get
     *
     * Route default property calling
     *
     * @param   string $property Property name
     *
     * @final   This method can't be overwrite
     * @access  public
     * @return  mixed
     */
    final public function __get( $property )
    {
        $property = strtolower( $property );

        if( property_exists( $this, $property ) )
        {
            return $this->{$property};
        }
        elseif( in_array( $property, array( 'forge', 'utility' ) ) )
        {
            return $this->_load_extension_class( $property );
        }
    }

    // ------------------------------------------------------------------------

    /**
     * Load Extension Class
     *
     * @param   string $property Property index name
     *
     * @final   This method can't be overwrite
     * @access  public
     * @return  mixed
     */
    final protected function _load_extension_class( $property )
    {
        if( ! isset( $this->{$property} ) )
        {
            $namespace = str_replace( 'Connection', '', get_called_class() );

            // Initialize Query Builder
            $extend_class_name = $namespace . ucfirst( $property );

            if( class_exists( $extend_class_name ) )
            {
                $this->{$property} = new $extend_class_name( $this );
            }
        }

        return $this->{$property};
    }

    // ------------------------------------------------------------------------

    /**
     * Create DSN
     *
     * Create DSN Connection String
     *
     * @access  protected
     * @return  void
     */
    abstract protected function _create_dsn();

    // ------------------------------------------------------------------------

    public function reconnect()
    {

    }

    public function disconnect()
    {

    }

    /**
     * Is Connected
     *
     * Determine is connection has been established
     *
     * @access  public
     * @return  bool
     */
    public function is_connected()
    {
        if( $this->pdo instanceof \PDO )
        {
            return TRUE;
        }

        return FALSE;
    }

    // ------------------------------------------------------------------------

    public function query( $sql, $params = array() )
    {
        return new Result( $this, $sql, $params );
    }

    /**
     * Execute
     *
     * Execute PDO Query
     *
     * @param   string $sql SQL Query
     *
     * @access  public
     * @return  \PDOStatement
     * @throws  \Exception
     */
    public function execute( $sql = NULL, $params = array() )
    {
        if( is_null( $sql ) )
        {
            $sql = $this->_builder->get_string();
            $params = $this->_builder->get_params();
        }

        $key = @count( static::$_registry[ 'queries' ] ) + 1;

        static::$_registry[ 'queries' ][ $key ][ 'string' ] = $sql;
        static::$_registry[ 'queries' ][ $key ][ 'params' ] = $params;
        static::$_registry[ 'queries' ][ $key ][ 'start' ] = array(
            'time'   => microtime( TRUE ),
            'memory' => memory_get_usage( TRUE ),
        );

        $this->_statement = $this->pdo->prepare( $sql );

        if( $this->transaction_enabled === TRUE )
        {
            $this->transaction_begin();
        }

        try
        {
            if( empty( $params ) AND count( $params ) == 0 )
            {
                $this->_statement->execute();
            }
            else
            {

                $this->_statement->execute( $params );
            }

            if( $this->transaction_enabled === TRUE )
            {
                $this->transaction_commit();
            }

            static::$_registry[ 'queries' ][ $key ][ 'end' ] = array(
                'time'   => microtime( TRUE ),
                'memory' => memory_get_usage( TRUE ),
            );

            $this->_builder = new Query( $this );

            return $this->_statement;
        }
        catch( Exception $e )
        {
            if( $this->transaction_enabled === TRUE )
            {
                $this->transaction_rollback();
            }

            $e->setStatement( $sql );

            throw new Exception( $e->getMessage() );
        }
    }

    // ------------------------------------------------------------------------

    /**
     * List Database
     *
     * Returns an array of database names
     *
     * @access  public
     * @return  array
     */
    abstract public function list_database();

    // ------------------------------------------------------------------------

    /**
     * Database Exists
     *
     * Determine if a particular database exists
     *
     * @param   string $database Database name
     *
     * @access  public
     * @return  bool
     */
    public function database_exists( $database )
    {
        if( in_array( $database, $this->list_database() ) )
        {
            return TRUE;
        }

        return FALSE;
    }

    // ------------------------------------------------------------------------

    /**
     * List Tables
     *
     * Returns an array of table names
     *
     * @access  public
     * @return  array
     */
    abstract public function list_tables();

    // ------------------------------------------------------------------------

    /**
     * Table Exists
     *
     * Determine if a particular table exists
     *
     * @param   string $table
     *
     * @access  public
     * @return  bool
     */
    public function table_exists( $table )
    {
        $table = ( empty( $this->prefix ) || strpos( $table, $this->prefix ) !== FALSE ) ? $table : $this->prefix . $table;

        if( in_array( $table, $this->list_tables() ) )
        {
            return TRUE;
        }

        return FALSE;
    }

    // ------------------------------------------------------------------------

    /**
     * Table Metadata
     *
     * Get sql table metadata
     *
     * @param   string $table
     *
     * @access  public
     * @return  \O2System\DB\Metadata\Table|bool
     */
    public function table_metadata( $table )
    {
        if( ! isset( static::$_registry[ 'tables' ] ) )
        {
            $this->list_tables();
        }

        return isset( static::$_registry[ 'tables' ][ $table ] ) ? static::$_registry[ 'tables' ][ $table ] : NULL;
    }

    // ------------------------------------------------------------------------

    /**
     * List Fields
     *
     * List of table fields
     *
     * @param    string $table Database table name
     *
     * @access  public
     * @return  array
     */
    public function list_fields( $table )
    {
        if( ! isset( static::$_registry[ 'tables' ] ) )
        {
            $this->list_tables();
        }

        if( isset( static::$_registry[ 'tables' ][ $table ] ) )
        {
            $table = static::$_registry[ 'tables' ][ $table ];

            return empty( $table->fields ) ? array() : array_keys( $table->fields );
        }

        return NULL;
    }

    // ------------------------------------------------------------------------

    /**
     * Field Exists
     *
     * Determine if a particular field exists
     *
     * @param   string $field Database table field name
     * @param   string $table Database table name
     *
     * @access  public
     * @return  bool
     */
    public function field_exists( $field, $table )
    {
        $fields = $this->list_fields( $table );

        if( in_array( $field, $fields ) )
        {
            return TRUE;
        }

        return FALSE;
    }

    // ------------------------------------------------------------------------

    /**
     * Field Metadata
     *
     * Get sql table field metadata
     *
     * @param   string $field Database table field name
     * @param   string $table Database table name
     *
     * @access  public
     * @return  \O2System\DB\Metadata\Field|bool
     */
    public function field_metadata( $field, $table )
    {
        if( ! isset( static::$_registry[ 'tables' ] ) )
        {
            $this->list_tables();
        }

        if( isset( static::$_registry[ 'tables' ][ $table ] ) )
        {
            $table = static::$_registry[ 'tables' ][ $table ];

            return isset( $table->fields[ $field ] ) ? $table->fields[ $field ] : NULL;
        }

        return NULL;
    }

    // ------------------------------------------------------------------------

    /**
     * List Indexes
     *
     * List of indexes table fields
     *
     * @param    string $table Database table name
     *
     * @access  public
     * @return  array
     */
    public function list_indexes( $table )
    {
        if( ! isset( static::$_registry[ 'tables' ] ) )
        {
            $this->list_tables();
        }

        if( isset( static::$_registry[ 'tables' ][ $table ] ) )
        {
            $table = static::$_registry[ 'tables' ][ $table ];

            return empty( $table->indexes ) ? array() : array_keys( $table->indexes );
        }

        return NULL;
    }

    /**
     * Count All
     *
     * Count all records in specified database table
     *
     * @param   string $table
     *
     * @access  public
     * @return  int
     */
    public function count_rows( $table )
    {
        $query = $this->query( 'SELECT COUNT(*) AS num_rows FROM ' . $this->escape_identifier( $table ) );

        return $query->first_row()->num_rows;
    }

    // ------------------------------------------------------------------------

    /**
     * Affected Rows
     *
     * Number of affected rows
     *
     * @access  public
     * @return  int
     */
    public function affected_rows()
    {
        return (int)$this->_statement->rowCount();
    }

    // ------------------------------------------------------------------------

    public function last_query()
    {
        return end( static::$_registry[ 'queries' ] );
    }

    /**
     * Transaction Flag
     *
     * @access  public
     * @return  Connection
     */
    public function transaction_enabled( $enabled = TRUE )
    {
        $this->transaction_enabled = $enabled;

        return $this;
    }

    // ------------------------------------------------------------------------

    /**
     * Begin Transaction
     *
     * @access  public
     * @return  Connection
     */
    public function transaction_begin()
    {
        $this->pdo->beginTransaction();

        return $this;
    }

    // ------------------------------------------------------------------------

    /**
     * Commit Transaction
     *
     * @access  public
     * @return  Connection
     */
    public function transaction_commit()
    {
        $this->pdo->commit();

        return $this;
    }

    // ------------------------------------------------------------------------

    /**
     * Rollback Transaction
     *
     * @access  public
     * @return  Connection
     */
    public function transaction_rollback()
    {
        $this->pdo->rollBack();

        return $this;
    }

    // ------------------------------------------------------------------------

    /**
     * Get last insert id
     *
     * @access  public
     * @return  int
     */
    public function last_insert_id()
    {
        return $this->pdo->lastInsertId();
    }

    // ------------------------------------------------------------------------

    /**
     * Database platform
     *
     * @access public
     * @return string
     */
    public function platform()
    {
        return $this->platform;
    }

    // ------------------------------------------------------------------------

    /**
     * Database platform version
     *
     * @access  public
     * @return  string
     */
    public function version()
    {
        if( isset( static::$_registry[ 'version' ] ) )
        {
            return static::$_registry[ 'version' ];
        }

        // Not all subdrivers support the getAttribute() method
        try
        {
            return static::$_registry[ 'version' ] = $this->pdo->getAttribute( \PDO::ATTR_SERVER_VERSION );
        }
        catch( \PDOException $e )
        {
            $query = $this->query( 'SELECT VERSION() AS platform_version' );

            if( $query->num_rows() > 0 )
            {
                return static::$_registry[ 'version' ] = $query->first_row()->platform_version;
            }
        }
    }

    // ------------------------------------------------------------------------

    /**
     * Close database connection
     *
     * @access public
     * @return void
     */
    public function close()
    {
        $this->pdo = NULL;
    }

    // ------------------------------------------------------------------------

    /**
     * Platform-dependant string escape
     *
     * @param    string
     *
     * @return    string
     */
    public function escape_string( $string )
    {
        // Escape the string
        $string = $this->pdo->quote( $string );

        // If there are duplicated quotes, trim them away
        return ( strpos( $string, "''" ) !== FALSE )
            ? preg_replace( "/[']+/", "'", $string )
            : $string;
    }

    // --------------------------------------------------------------------

    /**
     * Flatten Array
     *
     * @param   array $array Array of strings
     * @param   bool  $quote Use quote flag
     *
     * @access  public
     * @return  string
     */
    public function escape_array( $array, $quote = TRUE )
    {
        if( ! empty( $array ) )
        {
            foreach( $array as $key => $value )
            {
                $flatten[ ] = $quote === TRUE ? $this->escape_string( $value ) : $value;
            }

            return implode( ', ', $flatten );
        }

        return NULL;
    }

    // ------------------------------------------------------------------------

    /**
     * Escape the SQL Identifier
     *
     * This function escapes column and table names
     *
     * @param    mixed
     *
     * @return    mixed
     */
    public function escape_identifier( $item )
    {
        if( $this->_escape_char === '' OR empty( $item ) OR in_array( $item, $this->_reserved_identifiers ) )
        {
            return $item;
        }
        elseif( is_array( $item ) )
        {
            foreach( $item as $key => $value )
            {
                $item[ $key ] = $this->escape_identifier( $value );
            }

            return $item;
        }
        // Avoid breaking functions and literal values inside queries
        elseif( ctype_digit( $item ) OR $item[ 0 ] === "'" OR ( $this->_escape_char !== '"' && $item[ 0 ] === '"' ) OR strpos( $item, '(' ) !== FALSE )
        {
            return $item;
        }

        static $preg_ec = array();

        if( empty( $preg_ec ) )
        {
            if( is_array( $this->_escape_char ) )
            {
                $preg_ec = array(
                    preg_quote( $this->_escape_char[ 0 ], '/' ),
                    preg_quote( $this->_escape_char[ 1 ], '/' ),
                    $this->_escape_char[ 0 ],
                    $this->_escape_char[ 1 ]
                );
            }
            else
            {
                $preg_ec[ 0 ] = $preg_ec[ 1 ] = preg_quote( $this->_escape_char, '/' );
                $preg_ec[ 2 ] = $preg_ec[ 3 ] = $this->_escape_char;
            }
        }

        foreach( $this->_reserved_identifiers as $id )
        {
            if( strpos( $item, '.' . $id ) !== FALSE )
            {
                return preg_replace( '/' . $preg_ec[ 0 ] . '?([^' . $preg_ec[ 1 ] . '\.]+)' . $preg_ec[ 1 ] . '?\./i', $preg_ec[ 2 ] . '$1' . $preg_ec[ 3 ] . '.', $item );
            }
        }

        return preg_replace( '/' . $preg_ec[ 0 ] . '?([^' . $preg_ec[ 1 ] . '\.]+)' . $preg_ec[ 1 ] . '?(\.)?/i', $preg_ec[ 2 ] . '$1' . $preg_ec[ 3 ] . '$2', $item );
    }
}