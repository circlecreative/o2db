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

namespace O2System\DB\Drivers\Sqlite;

// ------------------------------------------------------------------------

use O2System\DB\Interfaces\Forge as ForgeInterface;

/**
 * SQLite Driver Forge Class
 *
 * @package     O2DB
 * @subpackage  Drivers/Sqlite
 * @category    Driver Class
 * @author      Circle Creative Developer Team
 * @link        http://circle-creative.com/products/o2db.html
 */
class Forge extends ForgeInterface
{

    /**
     * Create Database
     *
     * @param   string $database Database name
     * @param   array  $options  Database configurations
     *
     * @access  public
     * @return  mixed
     */
    public function create_database( $database, array $options = array() )
    {
        if( strpos( $this->dsn, '::memory:' ) )
        {
            // not supported
            return FALSE;
        }
        else
        {
            $files = array();
            $path = pathinfo( $this->database, PATHINFO_DIRNAME ) . '/';
            $dbs = glob( $path . '*.db' );

            foreach( $dbs as $db )
            {
                $files[ ] = pathinfo( $db, PATHINFO_FILENAME );
            }

            if( isset( $options[ 'if_not_exists' ] ) AND $options[ 'if_not_exists' ] === TRUE )
            {
                if( ! in_array( $database, $files ) )
                {
                    $db = fopen( $path . $database . '.db', 'w' );

                    if( file_exists( $path . $database . '.db' ) )
                    {
                        return TRUE;
                    }
                }
            }
            else
            {
                if( in_array( $database, $files ) )
                {
                    unlink( $path . $database . '.db' );

                    $db = fopen( $path . $database . '.db', 'w' );

                    if( file_exists( $path . $database . '.db' ) )
                    {
                        return TRUE;
                    }
                }
            }

            return FALSE;
        }
    }

    // ------------------------------------------------------------------------

    /**
     * Drop Database
     *
     * @param   string $database Database name
     *
     * @access  public
     * @return  mixed
     */
    public function drop_database( $database )
    {
        if( strpos( $this->dsn, '::memory:' ) )
        {
            // not supported
            return FALSE;
        }
        else
        {
            $path = pathinfo( $this->database, PATHINFO_DIRNAME ) . '/';
            $dbs = glob( $path . '*.db' );

            foreach( $dbs as $db )
            {
                $file = pathinfo( $db, PATHINFO_FILENAME );

                if( $file === $database )
                {
                    unlink( $db );

                    return TRUE;
                    break;
                }
            }

            return FALSE;
        }
    }

    // ------------------------------------------------------------------------

    /**
     * Create Table
     *
     * @param   string $table   Database table name
     * @param   array  $options Table configurations
     *
     * @access  public
     * @return  mixed
     */
    public function create_table( $table, $options = array() )
    {
        $table = empty( $this->_conn->prefix ) ? trim( $table ) : $this->_conn->prefix . trim( $table );
        $table = '"' . $table . '"';

        foreach( $options as $key => $value )
        {
            if( ! in_array( $key, Schema::$valid_table_options ) )
            {
                unset( $options[ $key ] );
            }
        }

        if( ! isset( $options[ 'fields' ] ) )
        {
            $options[ 'fields' ] = $this->_fields;
        }

        if( isset( $options[ 'record_fields' ] ) AND $options[ 'record_fields' ] === TRUE )
        {
            $options[ 'fields' ] = array_merge_recursive( $options[ 'fields' ], Schema::$record_fields );
        }

        $attributes = array();
        $primary_keys = array();

        foreach( $options[ 'fields' ] as $name => $option )
        {
            $name = trim( $name );

            $attribute = array();
            $attribute[ $name ] = '"' . $name . '"';

            foreach( $option as $key => $value )
            {
                if( $key === 'type' )
                {
                    if( in_array( strtoupper( $value ), Schema::$valid_field_types ) )
                    {
                        $attribute[ $key ] = strtoupper( $value );
                    }
                }
                elseif( $key === 'length' )
                {
                    if( isset( $attribute[ 'type' ] ) )
                    {
                        $attribute[ 'type' ] = $attribute[ 'type' ] . '(' . $value . ')';
                    }
                }
                elseif( $key === 'primary_key' )
                {
                    if( isset( $option[ 'auto_increment' ] ) AND $option[ 'auto_increment' ] === TRUE )
                    {
                        $new_attribute[ ] = $attribute[ $name ] . ' INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL';
                        $attribute = $new_attribute;
                        unset( $new_attribute );
                    }
                    else
                    {
                        $primary_keys[ ] = $name . ' ASC';
                    }
                }
                elseif( $key === 'not_null' )
                {
                    if( $value === TRUE )
                    {
                        $attribute[ $key ] = 'NOT NULL';
                    }
                }
                elseif( $key === 'default' )
                {
                    $attribute[ $key ] = 'DEFAULT ' . $value;
                }
            }

            $attributes[ ] = implode( ' ', array_values( $attribute ) );
        }

        if( ! empty( $primary_keys ) )
        {
            $attributes[ ] = 'PRIMARY KEY (' . implode( ', ', $primary_keys ) . ')';
        }

        if( isset( $options[ 'if_not_exists' ] ) AND $options[ 'if_not_exists' ] === TRUE )
        {
            $sql = 'CREATE TABLE IF NOT EXISTS %s (%s)';
        }
        else
        {
            $sql = 'CREATE TABLE %s (%s)';
        }

        $sql = sprintf( $sql, $table, implode( ", ", $attributes ) );

        $this->_conn->execute( $sql );

        return $this->_conn->table_exists( $table );
    }

    // ------------------------------------------------------------------------

    /**
     * Truncate Table
     *
     * @param   string $table Database table name
     *
     * @access  public
     * @return  mixed
     */
    public function truncate_table( $table )
    {
        $sql = 'DELETE FROM ' . $this->_conn->escape_identifier( $table ) . '; VACUUM;';
        $this->_conn->execute( $sql );

        return $this->_conn->affected_rows() != 0 ? TRUE : FALSE;
    }

    // ------------------------------------------------------------------------

    /**
     * Drop Table
     *
     * @param   string $table Database table name
     *
     * @access  public
     * @return  mixed
     */
    public function drop_table( $table )
    {
        $sql = 'DROP TABLE ' . $this->_conn->escape_identifier( $table );
        $this->_conn->execute( $sql );

        return $this->_conn->table_exists( $table ) === FALSE ? TRUE : FALSE;
    }

    // ------------------------------------------------------------------------

    /**
     * Rename Table
     *
     * @param   string $table     Old Database table name
     * @param   string $new_table New database table name
     *
     * @access  public
     * @return  mixed
     */
    public function rename_table( $table, $new_table )
    {
        $sql = 'ALTER TABLE %s RENAME TO %s';
        $sql = sprintf( $sql, $this->_conn->escape_identifier( $table ), $this->_conn->escape_identifier( $new_table ) );
        $this->_conn->execute($sql);

        return $this->_conn->affected_rows() != 0 ? TRUE : FALSE;
    }

    // ------------------------------------------------------------------------

    /**
     * Add Table Field
     *
     * @param   string $field   Database table field name
     * @param   string $table   Database table name
     * @param   array  $options Table configurations
     *
     * @access  public
     * @return  mixed
     */
    public function add_field( $field, $table, array $options = array() )
    {
        $fields = $this->_conn->list_fields( $table );

        if( in_array( $field, $fields ) )
        {
            return FALSE;
        }

        foreach( $options as $key => $value )
        {
            if( $key === 'type' )
            {
                if( in_array( strtoupper( $value ), Schema::$valid_field_types ) )
                {
                    $attribute[ $key ] = strtoupper( $value );
                }
            }
            elseif( $key === 'length' )
            {
                if( isset( $attribute[ 'type' ] ) )
                {
                    $attribute[ 'type' ] = $attribute[ 'type' ] . '(' . $value . ')';
                }
            }
            elseif( $key === 'primary_key' )
            {
                if( isset( $option[ 'auto_increment' ] ) AND $option[ 'auto_increment' ] === TRUE )
                {
                    $new_attribute[ ] = 'INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL';
                    $attribute = $new_attribute;
                    unset( $new_attribute );
                }
                else
                {
                    $attribute[ $key ] = 'PRIMARY KEY';
                }
            }
            elseif( $key === 'not_null' )
            {
                if( $value === TRUE )
                {
                    $attribute[ $key ] = 'NOT NULL';
                }
            }
            elseif( $key === 'default' )
            {
                $attribute[ $key ] = 'DEFAULT ' . $value;
            }
        }

        $sql = 'ALTER TABLE %s ADD COLUMN %s %s';
        $sql = sprintf( $sql, $this->_conn->escape_identifier( $table ), $this->_conn->escape_identifier( $field ), implode( ' ', array_values( $attribute ) ) );

        $this->_conn->execute($sql);

        return $this->_conn->field_exists($field, $table);
    }

    // ------------------------------------------------------------------------

    /**
     * Modify Table Fields
     *
     * @param   string $field   Database table field name
     * @param   string $table   Database table name
     * @param   array  $options Database table field configurations
     *
     * @access  public
     * @return  mixed
     */
    public function modify_field( $field, $table, array $options = array() )
    {
        // TODO: Implement modify_field() method.
    }

    // ------------------------------------------------------------------------

    /**
     * Drop Table Field
     *
     * @param   string $fields Database table field name
     * @param   string $table Database table name
     *
     * @access  public
     * @return  mixed
     */
    public function drop_field( $fields, $table )
    {
        // TODO: Implement drop_field() method.
    }

    // ------------------------------------------------------------------------
    /**
     * Add Table Primary Key Field
     *
     * @param   string $field   Database table field name
     * @param   string $table   Database table name
     * @param   array  $options Table configurations
     *
     * @access  public
     * @return  mixed
     */
    public function add_primary_field( $field, $table, array $options = array() )
    {
        // TODO: Implement add_primary_field() method.
    }

    /**
     * Add Table Index Field
     *
     * @param   string $field   Database table field name
     * @param   string $table   Database table name
     * @param   array  $options Table configurations
     *
     * @access  public
     * @return  mixed
     */
    public function add_index_field( $field, $table, array $options = array() )
    {
        // TODO: Implement add_index_field() method.
    }

    /**
     * Add Table Unique Field
     *
     * @param   string $field   Database table field name
     * @param   string $table   Database table name
     * @param   array  $options Table configurations
     *
     * @access  public
     * @return  mixed
     */
    public function add_unique_field( $field, $table, array $options = array() )
    {
        // TODO: Implement add_unique_field() method.
    }

    /**
     * Add Table Constraint Field
     *
     * @param   string $field   Database table field name
     * @param   string $table   Database table name
     * @param   array  $options Table configurations
     *
     * @access  public
     * @return  mixed
     */
    public function add_constraint_field( $field, $table, array $options = array() )
    {
        // TODO: Implement add_constraint_field() method.
    }
}