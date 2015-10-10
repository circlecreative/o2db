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

/**
 * Forge Interface Class
 *
 * @package     O2DB
 * @subpackage  Interfaces
 * @category    Interface Class
 * @author      Circle Creative Developer Team
 * @link        http://circle-creative.com/products/o2db.html
 */
abstract class Forge
{
    /**
     * Connection Class Object
     *
     * @access  protected
     * @type    Connection
     */
    protected $_conn;

    /**
     * Fields data
     *
     * @var    array
     */
    protected $_fields = array();

    /**
     * Keys data
     *
     * @var    array
     */
    protected $_keys = array();

    /**
     * Primary Keys data
     *
     * @var    array
     */
    protected $_primary_keys = array();

    /**
     * Database character set
     *
     * @var    string
     */
    public $db_char_set = '';

    // --------------------------------------------------------------------

    /**
     * CREATE DATABASE statement
     *
     * @var    string
     */
    protected $_create_database = 'CREATE DATABASE %s';

    /**
     * DROP DATABASE statement
     *
     * @var    string
     */
    protected $_drop_database = 'DROP DATABASE %s';

    /**
     * CREATE TABLE statement
     *
     * @var    string
     */
    protected $_create_table = "%s %s (%s\n)";

    /**
     * CREATE TABLE IF statement
     *
     * @var    string
     */
    protected $_create_table_if = 'CREATE TABLE IF NOT EXISTS';

    /**
     * CREATE TABLE keys flag
     *
     * Whether table keys are created from within the
     * CREATE TABLE statement.
     *
     * @var    bool
     */
    protected $_create_table_keys = FALSE;

    /**
     * DROP TABLE IF EXISTS statement
     *
     * @var    string
     */
    protected $_drop_table_if = 'DROP TABLE IF EXISTS';

    /**
     * RENAME TABLE statement
     *
     * @var    string
     */
    protected $_rename_table = 'ALTER TABLE %s RENAME TO %s;';

    /**
     * UNSIGNED support
     *
     * @var    bool|array
     */
    protected $_unsigned = TRUE;

    /**
     * NULL value representatin in CREATE/ALTER TABLE statements
     *
     * @var    string
     */
    protected $_null = '';

    /**
     * DEFAULT value representation in CREATE/ALTER TABLE statements
     *
     * @var    string
     */
    protected $_default = ' DEFAULT ';

    // --------------------------------------------------------------------

    /**
     * Class Constructor
     *
     * @param Connection $conn
     *
     * @access  public
     */
    public function __construct( Connection &$conn )
    {
        $this->_conn =& $conn;
    }

    // ------------------------------------------------------------------------

    /**
     * Create database
     *
     * @param    string $db_name
     *
     * @return    bool
     */
    public function create_database( $db_name )
    {
        if( $this->_create_database === FALSE )
        {
            return ( $this->_conn->debug_enabled ) ? Exception::show( 'db_unsupported_feature' ) : FALSE;
        }
        elseif( ! $this->_conn->execute( sprintf( $this->_create_database, $db_name, $this->_conn->charset, $this->_conn->collate ) ) )
        {
            return ( $this->_conn->debug_enabled ) ? Exception::show( 'db_unable_to_drop' ) : FALSE;
        }

        return TRUE;
    }

    // --------------------------------------------------------------------

    /**
     * Drop database
     *
     * @param    string $db_name
     *
     * @return    bool
     */
    public function drop_database( $db_name )
    {
        if( $this->_drop_database === FALSE )
        {
            return ( $this->_conn->debug_enabled ) ? Exception::show( 'db_unsupported_feature' ) : FALSE;
        }
        elseif( ! $this->_conn->execute( sprintf( $this->_drop_database, $db_name ) ) )
        {
            return ( $this->_conn->debug_enabled ) ? Exception::show( 'db_unable_to_drop' ) : FALSE;
        }

        if( ! empty( $this->_conn->data_cache[ 'db_names' ] ) )
        {
            $key = array_search( strtolower( $db_name ), array_map( 'strtolower', $this->_conn->data_cache[ 'db_names' ] ), TRUE );
            if( $key !== FALSE )
            {
                unset( $this->_conn->data_cache[ 'db_names' ][ $key ] );
            }
        }

        return TRUE;
    }

    // --------------------------------------------------------------------

    /**
     * Add Key
     *
     * @param    string $key
     * @param    bool   $primary
     *
     * @return    CI_DB_forge
     */
    public function add_key( $key, $primary = FALSE )
    {
        if( $primary === TRUE && is_array( $key ) )
        {
            foreach( $key as $one )
            {
                $this->add_key( $one, $primary );
            }

            return $this;
        }

        if( $primary === TRUE )
        {
            $this->_primary_keys[ ] = $key;
        }
        else
        {
            $this->_keys[ ] = $key;
        }

        return $this;
    }

    // --------------------------------------------------------------------

    /**
     * Add Field
     *
     * @param    array $field
     *
     * @return    CI_DB_forge
     */
    public function add_field( $field )
    {
        if( is_string( $field ) )
        {
            if( $field === 'id' )
            {
                $this->add_field( array(
                                      'id' => array(
                                          'type'           => 'INT',
                                          'constraint'     => 9,
                                          'auto_increment' => TRUE
                                      )
                                  ) );
                $this->add_key( 'id', TRUE );
            }
            else
            {
                if( strpos( $field, ' ' ) === FALSE )
                {
                    Exception::show( 'Field information is required for that operation.' );
                }

                $this->_fields[ ] = $field;
            }
        }

        if( is_array( $field ) )
        {
            $this->_fields = array_merge( $this->_fields, $field );
        }

        return $this;
    }

    // --------------------------------------------------------------------

    /**
     * Create Table
     *
     * @param    string $table         Table name
     * @param    bool   $if_not_exists Whether to add IF NOT EXISTS condition
     * @param    array  $attributes    Associative array of table attributes
     *
     * @return    bool
     */
    public function create_table( $table, $if_not_exists = FALSE, array $attributes = array() )
    {
        if( $table === '' )
        {
            Exception::show( 'A table name is required for that operation.' );
        }
        else
        {
            $table = $this->_conn->dbprefix . $table;
        }

        if( count( $this->_fields ) === 0 )
        {
            Exception::show( 'Field information is required.' );
        }

        $sql = $this->_create_table( $table, $if_not_exists, $attributes );

        if( is_bool( $sql ) )
        {
            $this->_reset();
            if( $sql === FALSE )
            {
                return ( $this->_conn->debug_enabled ) ? Exception::show( 'db_unsupported_feature' ) : FALSE;
            }
        }

        if( ( $result = $this->_conn->execute( $sql ) ) !== FALSE )
        {
            empty( $this->_conn->data_cache[ 'table_names' ] ) OR $this->_conn->data_cache[ 'table_names' ][ ] = $table;

            // Most databases don't support creating indexes from within the CREATE TABLE statement
            if( ! empty( $this->_keys ) )
            {
                for( $i = 0, $sqls = $this->_process_indexes( $table ), $c = count( $sqls ); $i < $c; $i++ )
                {
                    $this->_conn->execute( $sqls[ $i ] );
                }
            }
        }

        $this->_reset();

        return $result;
    }

    // --------------------------------------------------------------------

    /**
     * Create Table
     *
     * @param    string $table         Table name
     * @param    bool   $if_not_exists Whether to add 'IF NOT EXISTS' condition
     * @param    array  $attributes    Associative array of table attributes
     *
     * @return    mixed
     */
    protected function _create_table( $table, $if_not_exists, $attributes )
    {
        if( $if_not_exists === TRUE && $this->_create_table_if === FALSE )
        {
            if( $this->_conn->table_exists( $table ) )
            {
                return TRUE;
            }
            else
            {
                $if_not_exists = FALSE;
            }
        }

        $sql = ( $if_not_exists )
            ? sprintf( $this->_create_table_if, $this->_conn->escape_identifiers( $table ) )
            : 'CREATE TABLE';

        $columns = $this->_process_fields( TRUE );
        for( $i = 0, $c = count( $columns ); $i < $c; $i++ )
        {
            $columns[ $i ] = ( $columns[ $i ][ '_literal' ] !== FALSE )
                ? "\n\t" . $columns[ $i ][ '_literal' ]
                : "\n\t" . $this->_process_column( $columns[ $i ] );
        }

        $columns = implode( ',', $columns )
                   . $this->_process_primary_keys( $table );

        // Are indexes created from within the CREATE TABLE statement? (e.g. in MySQL)
        if( $this->_create_table_keys === TRUE )
        {
            $columns .= $this->_process_indexes( $table );
        }

        // _create_table will usually have the following format: "%s %s (%s\n)"
        $sql = sprintf( $this->_create_table . '%s',
                        $sql,
                        $this->_conn->escape_identifiers( $table ),
                        $columns,
                        $this->_create_table_attr( $attributes )
        );

        return $sql;
    }

    // --------------------------------------------------------------------

    /**
     * CREATE TABLE attributes
     *
     * @param    array $attributes Associative array of table attributes
     *
     * @return    string
     */
    protected function _create_table_attr( $attributes )
    {
        $sql = '';

        foreach( array_keys( $attributes ) as $key )
        {
            if( is_string( $key ) )
            {
                $sql .= ' ' . strtoupper( $key ) . ' ' . $attributes[ $key ];
            }
        }

        return $sql;
    }

    // --------------------------------------------------------------------

    /**
     * Drop Table
     *
     * @param    string $table_name Table name
     * @param    bool   $if_exists  Whether to add an IF EXISTS condition
     *
     * @return    bool
     */
    public function drop_table( $table_name, $if_exists = FALSE )
    {
        if( $table_name === '' )
        {
            return ( $this->_conn->debug_enabled ) ? Exception::show( 'db_table_name_required' ) : FALSE;
        }

        $query = $this->_drop_table( $this->_conn->dbprefix . $table_name, $if_exists );
        if( $query === FALSE )
        {
            return ( $this->_conn->debug_enabled ) ? Exception::show( 'db_unsupported_feature' ) : FALSE;
        }
        elseif( $query === TRUE )
        {
            return TRUE;
        }

        $query = $this->_conn->execute( $query );

        // Update table list cache
        if( $query && ! empty( $this->_conn->data_cache[ 'table_names' ] ) )
        {
            $key = array_search( strtolower( $this->_conn->dbprefix . $table_name ), array_map( 'strtolower', $this->_conn->data_cache[ 'table_names' ] ), TRUE );
            if( $key !== FALSE )
            {
                unset( $this->_conn->data_cache[ 'table_names' ][ $key ] );
            }
        }

        return $query;
    }

    // --------------------------------------------------------------------

    /**
     * Drop Table
     *
     * Generates a platform-specific DROP TABLE string
     *
     * @param    string $table     Table name
     * @param    bool   $if_exists Whether to add an IF EXISTS condition
     *
     * @return    string
     */
    protected function _drop_table( $table, $if_exists )
    {
        $sql = 'DROP TABLE';

        if( $if_exists )
        {
            if( $this->_drop_table_if === FALSE )
            {
                if( ! $this->_conn->table_exists( $table ) )
                {
                    return TRUE;
                }
            }
            else
            {
                $sql = sprintf( $this->_drop_table_if, $this->_conn->escape_identifiers( $table ) );
            }
        }

        return $sql . ' ' . $this->_conn->escape_identifiers( $table );
    }

    // --------------------------------------------------------------------

    /**
     * Rename Table
     *
     * @param    string $table_name     Old table name
     * @param    string $new_table_name New table name
     *
     * @return    bool
     */
    public function rename_table( $table_name, $new_table_name )
    {
        if( $table_name === '' OR $new_table_name === '' )
        {
            Exception::show( 'A table name is required for that operation.' );

            return FALSE;
        }
        elseif( $this->_rename_table === FALSE )
        {
            return ( $this->_conn->debug_enabled ) ? Exception::show( 'db_unsupported_feature' ) : FALSE;
        }

        $result = $this->_conn->execute( sprintf( $this->_rename_table,
                                                  $this->_conn->escape_identifiers( $this->_conn->dbprefix . $table_name ),
                                                  $this->_conn->escape_identifiers( $this->_conn->dbprefix . $new_table_name ) )
        );

        if( $result && ! empty( $this->_conn->data_cache[ 'table_names' ] ) )
        {
            $key = array_search( strtolower( $this->_conn->dbprefix . $table_name ), array_map( 'strtolower', $this->_conn->data_cache[ 'table_names' ] ), TRUE );
            if( $key !== FALSE )
            {
                $this->_conn->data_cache[ 'table_names' ][ $key ] = $this->_conn->dbprefix . $new_table_name;
            }
        }

        return $result;
    }

    // --------------------------------------------------------------------

    /**
     * Column Add
     *
     * @param    string $table Table name
     * @param    array  $field Column definition
     *
     * @return    bool
     */
    public function add_column( $table, $field )
    {
        // Work-around for literal column definitions
        is_array( $field ) OR $field = array( $field );

        foreach( array_keys( $field ) as $k )
        {
            $this->add_field( array( $k => $field[ $k ] ) );
        }

        $sqls = $this->_alter_table( 'ADD', $this->_conn->dbprefix . $table, $this->_process_fields() );
        $this->_reset();
        if( $sqls === FALSE )
        {
            return ( $this->_conn->debug_enabled ) ? Exception::show( 'db_unsupported_feature' ) : FALSE;
        }

        for( $i = 0, $c = count( $sqls ); $i < $c; $i++ )
        {
            if( $this->_conn->execute( $sqls[ $i ] ) === FALSE )
            {
                return FALSE;
            }
        }

        return TRUE;
    }

    // --------------------------------------------------------------------

    /**
     * Column Drop
     *
     * @param    string $table       Table name
     * @param    string $column_name Column name
     *
     * @return    bool
     */
    public function drop_column( $table, $column_name )
    {
        $sql = $this->_alter_table( 'DROP', $this->_conn->dbprefix . $table, $column_name );
        if( $sql === FALSE )
        {
            return ( $this->_conn->debug_enabled ) ? Exception::show( 'db_unsupported_feature' ) : FALSE;
        }

        return $this->_conn->execute( $sql );
    }

    // --------------------------------------------------------------------

    /**
     * Column Modify
     *
     * @param    string $table Table name
     * @param    string $field Column definition
     *
     * @return    bool
     */
    public function modify_column( $table, $field )
    {
        // Work-around for literal column definitions
        is_array( $field ) OR $field = array( $field );

        foreach( array_keys( $field ) as $k )
        {
            $this->add_field( array( $k => $field[ $k ] ) );
        }

        if( count( $this->_fields ) === 0 )
        {
            Exception::show( 'Field information is required.' );
        }

        $sqls = $this->_alter_table( 'CHANGE', $this->_conn->dbprefix . $table, $this->_process_fields() );
        $this->_reset();
        if( $sqls === FALSE )
        {
            return ( $this->_conn->debug_enabled ) ? Exception::show( 'db_unsupported_feature' ) : FALSE;
        }

        for( $i = 0, $c = count( $sqls ); $i < $c; $i++ )
        {
            if( $this->_conn->execute( $sqls[ $i ] ) === FALSE )
            {
                return FALSE;
            }
        }

        return TRUE;
    }

    // --------------------------------------------------------------------

    /**
     * ALTER TABLE
     *
     * @param    string $alter_type ALTER type
     * @param    string $table      Table name
     * @param    mixed  $field      Column definition
     *
     * @return    string|string[]
     */
    protected function _alter_table( $alter_type, $table, $field )
    {
        $sql = 'ALTER TABLE ' . $this->_conn->escape_identifiers( $table ) . ' ';

        // DROP has everything it needs now.
        if( $alter_type === 'DROP' )
        {
            return $sql . 'DROP COLUMN ' . $this->_conn->escape_identifiers( $field );
        }

        $sql .= ( $alter_type === 'ADD' )
            ? 'ADD '
            : $alter_type . ' COLUMN ';

        $sqls = array();
        for( $i = 0, $c = count( $field ); $i < $c; $i++ )
        {
            $sqls[ ] = $sql
                       . ( $field[ $i ][ '_literal' ] !== FALSE ? $field[ $i ][ '_literal' ] : $this->_process_column( $field[ $i ] ) );
        }

        return $sqls;
    }

    // --------------------------------------------------------------------

    /**
     * Process fields
     *
     * @param    bool $create_table
     *
     * @return    array
     */
    protected function _process_fields( $create_table = FALSE )
    {
        $fields = array();

        foreach( $this->_fields as $key => $attributes )
        {
            if( is_int( $key ) && ! is_array( $attributes ) )
            {
                $fields[ ] = array( '_literal' => $attributes );
                continue;
            }

            $attributes = array_change_key_case( $attributes, CASE_UPPER );

            if( $create_table === TRUE && empty( $attributes[ 'TYPE' ] ) )
            {
                continue;
            }

            isset( $attributes[ 'TYPE' ] ) && $this->_attr_type( $attributes );

            $field = array(
                'name'           => $key,
                'new_name'       => isset( $attributes[ 'NAME' ] ) ? $attributes[ 'NAME' ] : NULL,
                'type'           => isset( $attributes[ 'TYPE' ] ) ? $attributes[ 'TYPE' ] : NULL,
                'length'         => '',
                'unsigned'       => '',
                'null'           => '',
                'unique'         => '',
                'default'        => '',
                'auto_increment' => '',
                '_literal'       => FALSE
            );

            isset( $attributes[ 'TYPE' ] ) && $this->_attr_unsigned( $attributes, $field );

            if( $create_table === FALSE )
            {
                if( isset( $attributes[ 'AFTER' ] ) )
                {
                    $field[ 'after' ] = $attributes[ 'AFTER' ];
                }
                elseif( isset( $attributes[ 'FIRST' ] ) )
                {
                    $field[ 'first' ] = (bool)$attributes[ 'FIRST' ];
                }
            }

            $this->_attr_default( $attributes, $field );

            if( isset( $attributes[ 'NULL' ] ) )
            {
                if( $attributes[ 'NULL' ] === TRUE )
                {
                    $field[ 'null' ] = empty( $this->_null ) ? '' : ' ' . $this->_null;
                }
                else
                {
                    $field[ 'null' ] = ' NOT NULL';
                }
            }
            elseif( $create_table === TRUE )
            {
                $field[ 'null' ] = ' NOT NULL';
            }

            $this->_attr_auto_increment( $attributes, $field );
            $this->_attr_unique( $attributes, $field );

            if( isset( $attributes[ 'COMMENT' ] ) )
            {
                $field[ 'comment' ] = $this->_conn->escape( $attributes[ 'COMMENT' ] );
            }

            if( isset( $attributes[ 'TYPE' ] ) && ! empty( $attributes[ 'CONSTRAINT' ] ) )
            {
                switch( strtoupper( $attributes[ 'TYPE' ] ) )
                {
                    case 'ENUM':
                    case 'SET':
                        $attributes[ 'CONSTRAINT' ] = $this->_conn->escape( $attributes[ 'CONSTRAINT' ] );
                        $field[ 'length' ] = is_array( $attributes[ 'CONSTRAINT' ] )
                            ? "('" . implode( "','", $attributes[ 'CONSTRAINT' ] ) . "')"
                            : '(' . $attributes[ 'CONSTRAINT' ] . ')';
                        break;
                    default:
                        $field[ 'length' ] = is_array( $attributes[ 'CONSTRAINT' ] )
                            ? '(' . implode( ',', $attributes[ 'CONSTRAINT' ] ) . ')'
                            : '(' . $attributes[ 'CONSTRAINT' ] . ')';
                        break;
                }
            }

            $fields[ ] = $field;
        }

        return $fields;
    }

    // --------------------------------------------------------------------

    /**
     * Process column
     *
     * @param    array $field
     *
     * @return    string
     */
    protected function _process_column( $field )
    {
        return $this->_conn->escape_identifiers( $field[ 'name' ] )
               . ' ' . $field[ 'type' ] . $field[ 'length' ]
               . $field[ 'unsigned' ]
               . $field[ 'default' ]
               . $field[ 'null' ]
               . $field[ 'auto_increment' ]
               . $field[ 'unique' ];
    }

    // --------------------------------------------------------------------

    /**
     * Field attribute TYPE
     *
     * Performs a data type mapping between different databases.
     *
     * @param    array &$attributes
     *
     * @return    void
     */
    protected function _attr_type( &$attributes )
    {
        // Usually overridden by drivers
    }

    // --------------------------------------------------------------------

    /**
     * Field attribute UNSIGNED
     *
     * Depending on the _unsigned property value:
     *
     *    - TRUE will always set $field['unsigned'] to 'UNSIGNED'
     *    - FALSE will always set $field['unsigned'] to ''
     *    - array(TYPE) will set $field['unsigned'] to 'UNSIGNED',
     *        if $attributes['TYPE'] is found in the array
     *    - array(TYPE => UTYPE) will change $field['type'],
     *        from TYPE to UTYPE in case of a match
     *
     * @param    array &$attributes
     * @param    array &$field
     *
     * @return    void
     */
    protected function _attr_unsigned( &$attributes, &$field )
    {
        if( empty( $attributes[ 'UNSIGNED' ] ) OR $attributes[ 'UNSIGNED' ] !== TRUE )
        {
            return;
        }

        // Reset the attribute in order to avoid issues if we do type conversion
        $attributes[ 'UNSIGNED' ] = FALSE;

        if( is_array( $this->_unsigned ) )
        {
            foreach( array_keys( $this->_unsigned ) as $key )
            {
                if( is_int( $key ) && strcasecmp( $attributes[ 'TYPE' ], $this->_unsigned[ $key ] ) === 0 )
                {
                    $field[ 'unsigned' ] = ' UNSIGNED';

                    return;
                }
                elseif( is_string( $key ) && strcasecmp( $attributes[ 'TYPE' ], $key ) === 0 )
                {
                    $field[ 'type' ] = $key;

                    return;
                }
            }

            return;
        }

        $field[ 'unsigned' ] = ( $this->_unsigned === TRUE ) ? ' UNSIGNED' : '';
    }

    // --------------------------------------------------------------------

    /**
     * Field attribute DEFAULT
     *
     * @param    array &$attributes
     * @param    array &$field
     *
     * @return    void
     */
    protected function _attr_default( &$attributes, &$field )
    {
        if( $this->_default === FALSE )
        {
            return;
        }

        if( array_key_exists( 'DEFAULT', $attributes ) )
        {
            if( $attributes[ 'DEFAULT' ] === NULL )
            {
                $field[ 'default' ] = empty( $this->_null ) ? '' : $this->_default . $this->_null;

                // Override the NULL attribute if that's our default
                $attributes[ 'NULL' ] = TRUE;
                $field[ 'null' ] = empty( $this->_null ) ? '' : ' ' . $this->_null;
            }
            else
            {
                $field[ 'default' ] = $this->_default . $this->_conn->escape( $attributes[ 'DEFAULT' ] );
            }
        }
    }

    // --------------------------------------------------------------------

    /**
     * Field attribute UNIQUE
     *
     * @param    array &$attributes
     * @param    array &$field
     *
     * @return    void
     */
    protected function _attr_unique( &$attributes, &$field )
    {
        if( ! empty( $attributes[ 'UNIQUE' ] ) && $attributes[ 'UNIQUE' ] === TRUE )
        {
            $field[ 'unique' ] = ' UNIQUE';
        }
    }

    // --------------------------------------------------------------------

    /**
     * Field attribute AUTO_INCREMENT
     *
     * @param    array &$attributes
     * @param    array &$field
     *
     * @return    void
     */
    protected function _attr_auto_increment( &$attributes, &$field )
    {
        if( ! empty( $attributes[ 'AUTO_INCREMENT' ] ) && $attributes[ 'AUTO_INCREMENT' ] === TRUE && stripos( $field[ 'type' ], 'int' ) !== FALSE )
        {
            $field[ 'auto_increment' ] = ' AUTO_INCREMENT';
        }
    }

    // --------------------------------------------------------------------

    /**
     * Process primary keys
     *
     * @param    string $table Table name
     *
     * @return    string
     */
    protected function _process_primary_keys( $table )
    {
        $sql = '';

        for( $i = 0, $c = count( $this->_primary_keys ); $i < $c; $i++ )
        {
            if( ! isset( $this->_fields[ $this->_primary_keys[ $i ] ] ) )
            {
                unset( $this->_primary_keys[ $i ] );
            }
        }

        if( count( $this->_primary_keys ) > 0 )
        {
            $sql .= ",\n\tCONSTRAINT " . $this->_conn->escape_identifiers( 'pk_' . $table )
                    . ' PRIMARY KEY(' . implode( ', ', $this->_conn->escape_identifiers( $this->_primary_keys ) ) . ')';
        }

        return $sql;
    }

    // --------------------------------------------------------------------

    /**
     * Process indexes
     *
     * @param    string $table
     *
     * @return    string
     */
    protected function _process_indexes( $table )
    {
        $sqls = array();

        for( $i = 0, $c = count( $this->_keys ); $i < $c; $i++ )
        {
            if( is_array( $this->_keys[ $i ] ) )
            {
                for( $i2 = 0, $c2 = count( $this->_keys[ $i ] ); $i2 < $c2; $i2++ )
                {
                    if( ! isset( $this->_fields[ $this->_keys[ $i ][ $i2 ] ] ) )
                    {
                        unset( $this->_keys[ $i ][ $i2 ] );
                        continue;
                    }
                }
            }
            elseif( ! isset( $this->_fields[ $this->_keys[ $i ] ] ) )
            {
                unset( $this->_keys[ $i ] );
                continue;
            }

            is_array( $this->_keys[ $i ] ) OR $this->_keys[ $i ] = array( $this->_keys[ $i ] );

            $sqls[ ] = 'CREATE INDEX ' . $this->_conn->escape_identifiers( $table . '_' . implode( '_', $this->_keys[ $i ] ) )
                       . ' ON ' . $this->_conn->escape_identifiers( $table )
                       . ' (' . implode( ', ', $this->_conn->escape_identifiers( $this->_keys[ $i ] ) ) . ');';
        }

        return $sqls;
    }

    // --------------------------------------------------------------------

    /**
     * Reset
     *
     * Resets table creation vars
     *
     * @return    void
     */
    protected function _reset()
    {
        $this->_fields = $this->_keys = $this->_primary_keys = array();
    }
}
