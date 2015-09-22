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

namespace O2System\O2DB\Drivers\Pdo;

// ------------------------------------------------------------------------

use O2System\O2DB\Interfaces\Result as ResultInterface;

/**
 * PDO Database Result
 *
 * @author      Circle Creative Developer Team
 */
class Result extends ResultInterface
{
    /**
     * Number of rows in the result set
     *
     * @access  public
     * @return  int
     */
    public function num_rows()
    {
        if( is_int( $this->num_rows ) )
        {
            return $this->num_rows;
        }
        elseif( count( $this->result_array ) > 0 )
        {
            return $this->num_rows = count( $this->result_array );
        }
        elseif( count( $this->result_object ) > 0 )
        {
            return $this->num_rows = count( $this->result_object );
        }
        elseif( ( $num_rows = $this->id_result->rowCount() ) > 0 )
        {
            return $this->num_rows = $num_rows;
        }

        return $this->num_rows = count( $this->result_array() );
    }

    // --------------------------------------------------------------------

    /**
     * Fetch Field Names
     *
     * Generates an array of column names
     *
     * @access  public
     * @return  bool
     */
    public function list_fields()
    {
        $field_names = array();
        for( $i = 0, $c = $this->num_fields(); $i < $c; $i++ )
        {
            // Might trigger an E_WARNING due to not all subdrivers
            // supporting getColumnMeta()
            $field_names[ $i ] = @$this->id_result->getColumnMeta( $i );
            $field_names[ $i ] = $field_names[ $i ][ 'name' ];
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
        return $this->id_result->columnCount();
    }

    // --------------------------------------------------------------------

    /**
     * Field data
     *
     * Generates an array of objects containing field meta-data
     *
     * @access  public
     * @return  array
     * @throws  \Exception
     */
    public function field_data()
    {
        try
        {
            $data = array();

            for( $i = 0, $c = $this->num_fields(); $i < $c; $i++ )
            {
                $field = $this->id_result->getColumnMeta( $i );

                $data[ $i ] = new \stdClass();
                $data[ $i ]->name = $field[ 'name' ];
                $data[ $i ]->type = $field[ 'native_type' ];
                $data[ $i ]->max_length = ( $field[ 'len' ] > 0 ) ? $field[ 'len' ] : NULL;
                $data[ $i ]->primary_key = (int)( ! empty( $field[ 'flags' ] ) && in_array( 'primary_key', $field[ 'flags' ], TRUE ) );
            }

            return $data;
        }
        catch( Exception $e )
        {
            if( $this->_driver->debug_enabled )
            {
                throw new \Exception('Unsupported feature of the database platform you are using.');
            }

            return FALSE;
        }
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
            $this->id_result = FALSE;
        }
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
        return $this->id_result->fetch( PDO::FETCH_ASSOC );
    }

    // --------------------------------------------------------------------

    /**
     * Result - object
     *
     * Returns the result set as an object
     *
     * @param    string $class_name
     *
     * @access  protected
     * @return  object
     */
    protected function _fetch_object( $class_name = '\stdClass' )
    {
        return $this->id_result->fetchObject( $class_name );
    }

}
