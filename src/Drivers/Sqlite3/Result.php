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

namespace O2System\O2DB\Drivers\Sqlite3;

// ------------------------------------------------------------------------

use O2System\O2DB\Interfaces\Result as ResultInterface;

/**
 * Sqlite3 Database Result
 *
 * @author      Circle Creative Developer Team
 */
class Result extends ResultInterface
{

    /**
     * Fetch Field Names
     *
     * Generates an array of column names
     *
     * @access  public
     * @return  array
     */
    public function list_fields()
    {
        $field_names = array();
        for( $i = 0, $c = $this->num_fields(); $i < $c; $i++ )
        {
            $field_names[ ] = $this->id_result->columnName( $i );
        }

        return $field_names;
    }

    // --------------------------------------------------------------------

    /**
     * Number of fields in the result set
     *
     * @access  public
     * @return  int
     */
    public function num_fields()
    {
        return $this->id_result->numColumns();
    }

    // --------------------------------------------------------------------

    /**
     * Field data
     *
     * Generates an array of objects containing field meta-data
     *
     * @access  public
     * @return  array
     */
    public function field_data()
    {
        static $data_types = array(
            SQLITE3_INTEGER => 'integer',
            SQLITE3_FLOAT   => 'float',
            SQLITE3_TEXT    => 'text',
            SQLITE3_BLOB    => 'blob',
            SQLITE3_NULL    => 'null'
        );

        $data = array();
        for( $i = 0, $c = $this->num_fields(); $i < $c; $i++ )
        {
            $data[ $i ] = new \stdClass();
            $data[ $i ]->name = $this->id_result->columnName( $i );

            $type = $this->id_result->columnType( $i );
            $data[ $i ]->type = isset( $data_types[ $type ] ) ? $data_types[ $type ] : $type;

            $data[ $i ]->max_length = NULL;
        }

        return $data;
    }

    // --------------------------------------------------------------------

    /**
     * Free the result
     *
     * @access  public
     * @return  void
     */
    public function free_result()
    {
        if( is_object( $this->id_result ) )
        {
            $this->id_result->finalize();
            $this->id_result = NULL;
        }
    }

    // --------------------------------------------------------------------

    /**
     * Data Seek
     *
     * Moves the internal pointer to the desired offset. We call
     * this internally before fetching results to make sure the
     * result set starts at zero.
     *
     * @access  public
     *
     * @param   int $n (ignored)
     *
     * @return  array
     */
    public function data_seek( $n = 0 )
    {
        // Only resetting to the start of the result set is supported
        return ( $n > 0 ) ? FALSE : $this->id_result->reset();
    }

    // --------------------------------------------------------------------

    /**
     * Result - associative array
     *
     * Returns the result set as an array
     *
     * @access  protected
     * @return  array
     */
    protected function _fetch_assoc()
    {
        return $this->id_result->fetchArray( SQLITE3_ASSOC );
    }

    // --------------------------------------------------------------------

    /**
     * Result - object
     *
     * Returns the result set as an object
     *
     * @param   string $class_name
     *
     * @access  protected
     * @return  object
     */
    protected function _fetch_object( $class_name = '\stdClass' )
    {
        // No native support for fetching rows as objects
        if( ( $row = $this->id_result->fetchArray( SQLITE3_ASSOC ) ) === FALSE )
        {
            return FALSE;
        }
        elseif( $class_name === '\stdClass' )
        {
            return (object)$row;
        }

        $class_name = new $class_name();
        foreach( array_keys( $row ) as $key )
        {
            $class_name->$key = $row[ $key ];
        }

        return $class_name;
    }

}