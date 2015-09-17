<?php
/**
 * O2System
 *
 * An open source application development framework for PHP 5.4 or newer
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
 * @package        O2System
 * @author         Steeven Andrian Salim
 * @copyright      Copyright (c) 2005 - 2014, PT. Lingkar Kreasi (Circle Creative).
 * @license        http://circle-creative.com/products/o2system/license.html
 * @license        http://opensource.org/licenses/MIT	MIT License
 * @link           http://circle-creative.com
 * @since          Version 2.0
 * @filesource
 */
namespace O2System\O2DB\Drivers\PDO\Subdrivers\Sqlite;
defined( 'BASEPATH' ) OR exit( 'No direct script access allowed' );


class Forge extends \O2System\O2DB\Interfaces\Forge
{

    /**
     * CREATE TABLE IF statement
     *
     * @var    string
     */
    protected $_create_table_if = 'CREATE TABLE IF NOT EXISTS';

    /**
     * DROP TABLE IF statement
     *
     * @var    string
     */
    protected $_drop_table_if = 'DROP TABLE IF EXISTS';

    /**
     * UNSIGNED support
     *
     * @var    bool|array
     */
    protected $_unsigned = FALSE;

    /**
     * NULL value representation in CREATE/ALTER TABLE statements
     *
     * @var    string
     */
    protected $_null = 'NULL';

    // --------------------------------------------------------------------

    /**
     * Class constructor
     *
     * @access public
     *
     * @param    object &$db Database object
     *
     * @return    void
     */
    public function __construct( &$db )
    {
        parent::__construct( $db );

        if( version_compare( $this->db->version(), '3.3', '<' ) )
        {
            $this->_create_table_if = FALSE;
        }
    }

    // --------------------------------------------------------------------

    /**
     * Create database
     *
     * @access public
     *
     * @param    string $db_name (ignored)
     *
     * @return    bool
     */
    public function create_database( $db_name = '' )
    {
        // In SQLite, a database is created when you connect to the database.
        // We'll return TRUE so that an error isn't generated
        return TRUE;
    }

    // --------------------------------------------------------------------

    /**
     * Drop database
     *
     * @access public
     *
     * @param    string $db_name (ignored)
     *
     * @return    bool
     */
    public function drop_database( $db_name = '' )
    {
        // In SQLite, a database is dropped when we delete a file
        if( file_exists( $this->db->database ) )
        {
            // We need to close the pseudo-connection first
            $this->db->close();
            if( ! @unlink( $this->db->database ) )
            {
                return $this->db->db_debug ? $this->db->display_error( 'db_unable_to_drop' ) : FALSE;
            }
            elseif( ! empty( $this->db->data_cache[ 'db_names' ] ) )
            {
                $key = array_search( strtolower( $this->db->database ), array_map( 'strtolower', $this->db->data_cache[ 'db_names' ] ), TRUE );
                if( $key !== FALSE )
                {
                    unset( $this->db->data_cache[ 'db_names' ][ $key ] );
                }
            }

            return TRUE;
        }

        return $this->db->db_debug ? $this->db->display_error( 'db_unable_to_drop' ) : FALSE;
    }

    // --------------------------------------------------------------------

    /**
     * ALTER TABLE
     *
     * @access protected
     *
     * @param    string $alter_type ALTER type
     * @param    string $table      Table name
     * @param    mixed  $field      Column definition
     *
     * @return    string|string[]
     */
    protected function _alter_table( $alter_type, $table, $field )
    {
        if( $alter_type === 'DROP' OR $alter_type === 'CHANGE' )
        {
            // drop_column():
            //	BEGIN TRANSACTION;
            //	CREATE TEMPORARY TABLE t1_backup(a,b);
            //	INSERT INTO t1_backup SELECT a,b FROM t1;
            //	DROP TABLE t1;
            //	CREATE TABLE t1(a,b);
            //	INSERT INTO t1 SELECT a,b FROM t1_backup;
            //	DROP TABLE t1_backup;
            //	COMMIT;

            return FALSE;
        }

        return parent::_alter_table( $alter_type, $table, $field );
    }

    // --------------------------------------------------------------------

    /**
     * Process column
     *
     * @access protected
     *
     * @param    array $field
     *
     * @return    string
     */
    protected function _process_column( $field )
    {
        return $this->db->escape_identifiers( $field[ 'name' ] )
               . ' ' . $field[ 'type' ]
               . $field[ 'auto_increment' ]
               . $field[ 'null' ]
               . $field[ 'unique' ]
               . $field[ 'default' ];
    }

    // --------------------------------------------------------------------

    /**
     * Field attribute TYPE
     *
     * Performs a data type mapping between different databases.
     *
     * @access protected
     *
     * @param    array &$attributes
     *
     * @return    void
     */
    protected function _attr_type( &$attributes )
    {
        switch( strtoupper( $attributes[ 'TYPE' ] ) )
        {
            case 'ENUM':
            case 'SET':
                $attributes[ 'TYPE' ] = 'TEXT';

                return;
            default:
                return;
        }
    }

    // --------------------------------------------------------------------

    /**
     * Field attribute AUTO_INCREMENT
     *
     * @access protected
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
            $field[ 'type' ] = 'INTEGER PRIMARY KEY';
            $field[ 'default' ] = '';
            $field[ 'null' ] = '';
            $field[ 'unique' ] = '';
            $field[ 'auto_increment' ] = ' AUTOINCREMENT';

            $this->primary_keys = array();
        }
    }

}

/* End of file Forge.php */
/* Location: ./o2system/libraries/database/drivers/PDO/subdrivers/SQLite/Forge.php */
