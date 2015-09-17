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
namespace O2System\O2DB\Drivers\PDO\Subdrivers\Ibm;
defined( 'BASEPATH' ) OR exit( 'No direct script access allowed' );


class Forge extends \O2System\O2DB\Interfaces\Forge
{

    /**
     * RENAME TABLE IF statement
     *
     * @var    string
     */
    protected $_rename_table = 'RENAME TABLE %s TO %s';

    /**
     * UNSIGNED support
     *
     * @var    array
     */
    protected $_unsigned = array(
        'SMALLINT' => 'INTEGER',
        'INT'      => 'BIGINT',
        'INTEGER'  => 'BIGINT'
    );

    /**
     * DEFAULT value representation in CREATE/ALTER TABLE statements
     *
     * @var    string
     */
    protected $_default = FALSE;

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
        if( $alter_type === 'CHANGE' )
        {
            $alter_type = 'MODIFY';
        }

        return parent::_alter_table( $alter_type, $table, $field );
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
            case 'TINYINT':
                $attributes[ 'TYPE' ] = 'SMALLINT';
                $attributes[ 'UNSIGNED' ] = FALSE;

                return;
            case 'MEDIUMINT':
                $attributes[ 'TYPE' ] = 'INTEGER';
                $attributes[ 'UNSIGNED' ] = FALSE;

                return;
            default:
                return;
        }
    }

    // --------------------------------------------------------------------

    /**
     * Field attribute UNIQUE
     *
     * @access protected
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

            // UNIQUE must be used with NOT NULL
            $field[ 'null' ] = ' NOT NULL';
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
        // Not supported
    }

}

/* End of file Driver.php */
/* Location: ./o2system/libraries/database/drivers/PDO/subdrivers/Ibm/Driver.php */
