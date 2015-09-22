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

namespace O2System\O2DB\Drivers\Pdo\Sql4d;

// ------------------------------------------------------------------------

use O2System\O2DB\Interfaces\Forge as ForgeInterface;

/**
 * PDO 4D Database Forge
 *
 * @author      Circle Creative Developer Team
 */
class Forge extends ForgeInterface
{

    /**
     * CREATE DATABASE statement
     *
     * @access  protected
     * @type    string
     */
    protected $_create_database = 'CREATE SCHEMA %s';

    /**
     * DROP DATABASE statement
     *
     * @access  protected
     * @type    string
     */
    protected $_drop_database = 'DROP SCHEMA %s';

    /**
     * CREATE TABLE IF statement
     *
     * @access  protected
     * @type    string
     */
    protected $_create_table_if = 'CREATE TABLE IF NOT EXISTS';

    /**
     * RENAME TABLE statement
     *
     * @access  protected
     * @type    string
     */
    protected $_rename_table = FALSE;

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
     * @type    array
     */
    protected $_unsigned = array(
        'INT16'    => 'INT',
        'SMALLINT' => 'INT',
        'INT'      => 'INT64',
        'INT32'    => 'INT64'
    );

    /**
     * DEFAULT value representation in CREATE/ALTER TABLE statements
     *
     * @access  protected
     * @type    string
     */
    protected $_default = FALSE;

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
        if( in_array( $alter_type, array( 'ADD', 'DROP' ), TRUE ) )
        {
            return parent::_alter_table( $alter_type, $table, $field );
        }

        // No method of modifying columns is supported
        return FALSE;
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
               . $field[ 'auto_increment' ];
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
            case 'INTEGER':
                $attributes[ 'TYPE' ] = 'INT';

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
     * Field attribute UNIQUE
     *
     * @param   array &$attributes
     * @param   array &$field
     *
     * @access  protected
     * @return  void
     */
    protected function _attr_unique( &$attributes, &$field )
    {
        if( ! empty( $attributes[ 'UNIQUE' ] ) && $attributes[ 'UNIQUE' ] === TRUE )
        {
            $field[ 'unique' ] = ' UNIQUE';

            // UNIQUE must be used with NOT NULL
            $field[ 'null' ] = ' NOT NULL';
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
        if( ! empty( $attributes[ 'AUTO_INCREMENT' ] ) && $attributes[ 'AUTO_INCREMENT' ] === TRUE )
        {
            if( stripos( $field[ 'type' ], 'int' ) !== FALSE )
            {
                $field[ 'auto_increment' ] = ' AUTO_INCREMENT';
            }
            elseif( strcasecmp( $field[ 'type' ], 'UUID' ) === 0 )
            {
                $field[ 'auto_increment' ] = ' AUTO_GENERATE';
            }
        }
    }

}
