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
namespace O2System\O2DB\Drivers\Cubrid;
defined( 'BASEPATH' ) OR exit( 'No direct script access allowed' );


/**
 * Database Driver Class
 *
 * @package        O2System
 * @subpackage     Drivers
 * @category       Database
 * @author         Steeven Andrian Salim
 * @link           http://o2system.center/framework/user-guide/libraries/database.htm
 */
class O2System\Libraries\DB_cubrid_result extends \O2System\O2DB\Interfaces\Result
{

    /**
     * Number of rows in the result set
     *
     * @return    int
     */
    public
    function num_rows()
    {
        return is_int( $this->num_rows )
            ? $this->num_rows
            : $this->num_rows = cubrid_num_rows( $this->result_id );
    }

    // --------------------------------------------------------------------

    /**
     * Fetch Field Names
     *
     * Generates an array of column names
     *
     * @access public
     *
     * @return    array
     */
    public
    function list_fields()
    {
        return cubrid_column_names( $this->result_id );
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
    public
    function field_data()
    {
        $retval = array();

        for( $i = 0, $c = $this->num_fields(); $i < $c; $i++ )
        {
            $retval[ $i ] = new \stdClass();
            $retval[ $i ]->name = cubrid_field_name( $this->result_id, $i );
            $retval[ $i ]->type = cubrid_field_type( $this->result_id, $i );
            $retval[ $i ]->max_length = cubrid_field_len( $this->result_id, $i );
            $retval[ $i ]->primary_key = (int)( strpos( cubrid_field_flags( $this->result_id, $i ), 'primary_key' ) !== FALSE );
        }

        return $retval;
    }

    // --------------------------------------------------------------------

    /**
     * Number of fields in the result set
     *
     * @access public
     *
     * @return    int
     */
    public
    function num_fields()
    {
        return cubrid_num_fields( $this->result_id );
    }

    // --------------------------------------------------------------------

    /**
     * Free the result
     *
     * @access public
     *
     * @return    void
     */
    public
    function free_result()
    {
        if( is_resource( $this->result_id ) OR
            ( get_resource_type( $this->result_id ) === 'Unknown' && preg_match( '/Resource id #/', strval( $this->result_id ) ) )
        )
        {
            cubrid_close_request( $this->result_id );
            $this->result_id = FALSE;
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
     * @access public
     *
     * @param    int $n
     *
     * @return    bool
     */
    public
    function data_seek( $n = 0 )
    {
        return cubrid_data_seek( $this->result_id, $n );
    }

    // --------------------------------------------------------------------

    /**
     * Result - associative array
     *
     * Returns the result set as an array
     *
     * @access public
     *
     * @return    array
     */
    protected
    function _fetch_assoc()
    {
        return cubrid_fetch_assoc( $this->result_id );
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
    protected
    function _fetch_object( $class_name = '\stdClass' )
    {
        return cubrid_fetch_object( $this->result_id, $class_name );
    }

}

/* End of file Result.php */
/* Location: ./o2system/libraries/database/drivers/Cubrid/Result.php */
