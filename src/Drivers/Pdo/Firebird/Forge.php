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

namespace O2System\O2DB\Drivers\Pdo\Firebird;

// ------------------------------------------------------------------------

use O2System\O2DB\Interfaces\Forge as ForgeInterface;

/**
 * PDO Firebird Database Forge
 *
 * @author      Circle Creative Developer Team
 */
class Forge extends ForgeInterface
{

    /**
     * RENAME TABLE statement
     *
     * @access  protected
     * @type    string
     */
    protected $_rename_table = FALSE;

    /**
     * UNSIGNED support
     *
     * @access  protected
     * @type    array
     */
    protected $_unsigned = array(
        'SMALLINT' => 'INTEGER',
        'INTEGER'  => 'INT64',
        'FLOAT'    => 'DOUBLE PRECISION'
    );

    /**
     * NULL value representation in CREATE/ALTER TABLE statements
     *
     * @access  protected
     * @type    string
     */
    protected $_null = 'NULL';

    // --------------------------------------------------------------------

    /**
     * Create database
     *
     * @param   string  $db_name
     *
     * @access  public
     * @return  string
     */
    public function create_database( $db_name )
    {
        // Firebird databases are flat files, so a path is required

        // Hostname is needed for remote access
        empty( $this->_driver->hostname ) OR $db_name = $this->hostname . ':' . $db_name;

        return parent::create_database( '"' . $db_name . '"' );
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
        if( ! ibase_drop_db( $this->id_connection ) )
        {
            if( $this->_driver->debug_enabled )
            {
                throw new \Exception( 'Unable to drop the specified database.' );
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
        if( in_array( $alter_type, array( 'DROP', 'ADD' ), TRUE ) )
        {
            return parent::_alter_table( $alter_type, $table, $field );
        }

        $sql = 'ALTER TABLE ' . $this->_driver->escape_identifiers( $table );
        $sqls = array();
        for( $i = 0, $c = count( $field ); $i < $c; $i++ )
        {
            if( $field[ $i ][ '_literal' ] !== FALSE )
            {
                return FALSE;
            }

            if( isset( $field[ $i ][ 'type' ] ) )
            {
                $sqls[ ] = $sql . ' ALTER COLUMN ' . $this->_driver->escape_identifiers( $field[ $i ][ 'name' ] )
                           . ' TYPE ' . $field[ $i ][ 'type' ] . $field[ $i ][ 'length' ];
            }

            if( ! empty( $field[ $i ][ 'default' ] ) )
            {
                $sqls[ ] = $sql . ' ALTER COLUMN ' . $this->_driver->escape_identifiers( $field[ $i ][ 'name' ] )
                           . ' SET DEFAULT ' . $field[ $i ][ 'default' ];
            }

            if( isset( $field[ $i ][ 'null' ] ) )
            {
                $sqls[ ] = 'UPDATE "RDB$RELATION_FIELDS" SET "RDB$NULL_FLAG" = '
                           . ( $field[ $i ][ 'null' ] === TRUE ? 'NULL' : '1' )
                           . ' WHERE "RDB$FIELD_NAME" = ' . $this->_driver->escape( $field[ $i ][ 'name' ] )
                           . ' && "RDB$RELATION_NAME" = ' . $this->_driver->escape( $table );
            }

            if( ! empty( $field[ $i ][ 'new_name' ] ) )
            {
                $sqls[ ] = $sql . ' ALTER COLUMN ' . $this->_driver->escape_identifiers( $field[ $i ][ 'name' ] )
                           . ' TO ' . $this->_driver->escape_identifiers( $field[ $i ][ 'new_name' ] );
            }
        }

        return $sqls;
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
               . ' ' . $field[ 'type' ] . $field[ 'length' ]
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
            case 'TINYINT':
                $attributes[ 'TYPE' ] = 'SMALLINT';
                $attributes[ 'UNSIGNED' ] = FALSE;

                return;
            case 'MEDIUMINT':
                $attributes[ 'TYPE' ] = 'INTEGER';
                $attributes[ 'UNSIGNED' ] = FALSE;

                return;
            case 'INT':
                $attributes[ 'TYPE' ] = 'INTEGER';

                return;
            case 'BIGINT':
                $attributes[ 'TYPE' ] = 'INT64';

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
        // Not supported
    }

}
