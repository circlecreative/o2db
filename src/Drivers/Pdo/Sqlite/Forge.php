<?php
/**
 * O2DB
 *
 * An open source PHP database engine driver for PHP 5.4 or newer
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
 * @license        http://circle-creative.com/products/o2db/license.html
 * @license        http://opensource.org/licenses/MIT   MIT License
 * @link           http://circle-creative.com/products/o2db.html
 * @filesource
 */
// ------------------------------------------------------------------------

namespace O2System\O2DB\Drivers\Pdo\Sqlite;

// ------------------------------------------------------------------------

use O2System\O2DB\Interfaces\Forge as ForgeInterface;

/**
 * PDO SQLite Database Forge
 *
 * @author      Circle Creative Developer Team
 */
class Forge extends ForgeInterface
{

    /**
     * CREATE TABLE IF statement
     *
     * @access  protected
     * @type    string
     */
    protected $_create_table_if = 'CREATE TABLE IF NOT EXISTS';

    /**
     * DROP TABLE IF statement
     *
     * @access  protected
     * @type    string
     */
    protected $_drop_table_if = 'DROP TABLE IF EXISTS';

    /**
     * UNSIGNED support
     *
     * @access  protected
     * @type    bool|array
     */
    protected $_unsigned = FALSE;

    /**
     * NULL value representation in CREATE/ALTER TABLE statements
     *
     * @access  protected
     * @type    string
     */
    protected $_null = 'NULL';

    // --------------------------------------------------------------------

    /**
     * Class constructor
     *
     * @param   object &$driver Database object
     *
     * @access  public
     */
    public function __construct( &$driver )
    {
        parent::__construct( $driver );

        if( version_compare( $this->_driver->version(), '3.3', '<' ) )
        {
            $this->_create_table_if = FALSE;
        }
    }

    // --------------------------------------------------------------------

    /**
     * Create database
     *
     * @param   string $db_name (ignored)
     *
     * @access  public
     * @return  bool
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
     * @param   string $db_name (ignored)
     *
     * @access  public
     * @return  bool
     * @throws  \Exception
     */
    public function drop_database( $db_name = '' )
    {
        // In SQLite, a database is dropped when we delete a file
        if( file_exists( $this->_driver->database ) )
        {
            // We need to close the pseudo-connection first
            $this->_driver->close();
            if( ! @unlink( $this->_driver->database ) )
            {
                if($this->_driver->debug_enabled)
                {
                    throw new \Exception('Unable to drop the specified database.');
                }

                return FALSE;
            }
            elseif( ! empty( $this->_driver->data_cache[ 'db_names' ] ) )
            {
                $key = array_search( strtolower( $this->_driver->database ), array_map( 'strtolower', $this->_driver->data_cache[ 'db_names' ] ), TRUE );
                if( $key !== FALSE )
                {
                    unset( $this->_driver->data_cache[ 'db_names' ][ $key ] );
                }
            }

            return TRUE;
        }

        return $this->_driver->db_debug ? $this->_driver->display_error( 'db_unable_to_drop' ) : FALSE;
    }

    // --------------------------------------------------------------------

    /**
     * ALTER TABLE
     *
     * @param   string $alter_type ALTER type
     * @param   string $table      Table name
     * @param   mixed  $field      Column definition
     *
     * @access  protected
     * @return  string|string[]
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
     * @param   array $field
     *
     * @access  protected
     * @return  string
     */
    protected function _process_column( $field )
    {
        return $this->_driver->escape_identifiers( $field[ 'name' ] )
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
     * @param   array &$attributes
     *
     * @access  protected
     * @return  void
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
     * @param   array &$attributes
     * @param   array &$field
     *
     * @access  protected
     * @return  void
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
