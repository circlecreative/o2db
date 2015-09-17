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
namespace O2System\O2DB\Drivers\PDO;
defined( 'BASEPATH' ) OR exit( 'No direct script access allowed' );


class Result extends \O2System\O2DB\Interfaces\Result
{

    /**
     * Number of rows in the result set
     *
     * @access public
     *
     * @return    int
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
        elseif( ( $num_rows = $this->result_id->rowCount() ) > 0 )
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
     * @access public
     *
     * @return    bool
     */
    public function list_fields()
    {
        $field_names = array();
        for( $i = 0, $c = $this->num_fields(); $i < $c; $i++ )
        {
            // Might trigger an E_WARNING due to not all subdrivers
            // supporting getColumnMeta()
            $field_names[ $i ] = @$this->result_id->getColumnMeta( $i );
            $field_names[ $i ] = $field_names[ $i ][ 'name' ];
        }

        return $field_names;
    }

    // --------------------------------------------------------------------

    /**
     * Number of fields in the result set
     *
     * @access public
     *
     * @return    int
     */
    public function num_fields()
    {
        return $this->result_id->columnCount();
    }

    // --------------------------------------------------------------------

    /**
     * Field data
     *
     * Generates an array of objects containing field meta-data
     *
     * @access public
     *
     * @return    array
     */
    public function field_data()
    {
        try
        {
            $retval = array();

            for( $i = 0, $c = $this->num_fields(); $i < $c; $i++ )
            {
                $field = $this->result_id->getColumnMeta( $i );

                $retval[ $i ] = new \stdClass();
                $retval[ $i ]->name = $field[ 'name' ];
                $retval[ $i ]->type = $field[ 'native_type' ];
                $retval[ $i ]->max_length = ( $field[ 'len' ] > 0 ) ? $field[ 'len' ] : NULL;
                $retval[ $i ]->primary_key = (int)( ! empty( $field[ 'flags' ] ) && in_array( 'primary_key', $field[ 'flags' ], TRUE ) );
            }

            return $retval;
        }
        catch( Exception $e )
        {
            if( $this->db->db_debug )
            {
                return $this->db->display_error( 'db_unsupported_feature' );
            }

            return FALSE;
        }
    }

    // --------------------------------------------------------------------

    /**
     * Free the result
     *
     * @access public
     *
     * @return    void
     */
    public function free_result()
    {
        if( is_object( $this->result_id ) )
        {
            $this->result_id = FALSE;
        }
    }

    // --------------------------------------------------------------------

    /**
     * Result - associative array
     *
     * Returns the result set as an array
     *
     * @access protected
     *
     * @return    array
     */
    protected function _fetch_assoc()
    {
        return $this->result_id->fetch( PDO::FETCH_ASSOC );
    }

    // --------------------------------------------------------------------

    /**
     * Result - object
     *
     * Returns the result set as an object
     *
     * @access protected
     *
     * @param    string $class_name
     *
     * @return    object
     */
    protected function _fetch_object( $class_name = '\stdClass' )
    {
        return $this->result_id->fetchObject( $class_name );
    }

}

/* End of file Result.php */
/* Location: ./o2system/libraries/database/drivers/PDO/Result.php */
