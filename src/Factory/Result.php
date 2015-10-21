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

namespace O2System\DB\Factory;

// ------------------------------------------------------------------------

/**
 * Result Interface Class
 *
 * @package     O2DB
 * @subpackage  Interfaces
 * @category    Interface Class
 * @author      Circle Creative Developer Team
 * @link        http://circle-creative.com/products/o2db.html
 */
class Result
{
    /**
     * Result Storage
     *
     * @static
     * @access  protected
     * @type    array
     */
    protected static $_storage;

    /**
     * Connection Class Object
     *
     * @access  protected
     * @type    Connection
     */
    protected $_driver;

    /**
     * Active Query Result
     *
     * @access  protected
     * @type    \PDO::PDOStatement
     */
    protected $_query;

    /**
     * Result Array
     *
     * @access  protected
     * @type    array
     */
    protected $_result_array = array();

    /**
     * Result Array
     *
     * @access  protected
     * @type    array
     */
    protected $_result_object = array();

    /**
     * Result Into Object
     *
     * @access  protected
     * @type    array
     */
    protected $_result_into = array();

    /**
     * Result Into Class
     *
     * @access  protected
     * @type    array
     */
    protected $_result_class = array();

    /**
     * Current Row Index
     *
     * @access  protected
     * @type    array
     */
    protected $_current_row = 1;

    // ------------------------------------------------------------------------

    /**
     * Class Constructor
     *
     * @param Connection $driver
     *
     * @access  public
     */
    public function __construct( &$driver, $sql = NULL, $params = array() )
    {
        $this->_driver =& $driver;

        $this->_query = $this->_driver->execute( $sql, $params );

        // Set Result Object
        $this->_query->setFetchMode( \PDO::FETCH_CLASS, '\O2System\DB\Metadata\Result' );

        $index = 0;
        while( $result = $this->_query->fetch() )
        {
            $index++;
            $this->_result_object[ $index ] = $result;
            $this->_result_array[ $index ] = $result->__toArray();
        }

        //print_code($this->_result_object);
    }

    // ------------------------------------------------------------------------

    /**
     * Result
     *
     * Return query result rows.
     *
     * @param   string $type Result type
     *
     * @access  public
     * @return  mixed
     */
    public function result( $type = NULL )
    {
        if( is_null( $type ) )
        {
            return $this->_result_object;
        }
        elseif( is_object( $type ) )
        {
            $class_name = get_class( $type );

            foreach( $this->_result_array as $index => $row )
            {
                if( $class_name === 'O2System\ORM\Factory\Result' )
                {
                    $reflection = new \ReflectionClass( $type );
                    $static_properties = $reflection->getStaticProperties();

                    $class_object = new $class_name( $static_properties['_model'] );
                }
                else
                {
                    $class_object = new $class_name;
                }

                foreach( $row as $key => $value )
                {
                    if( method_exists( $class_object, '__set' ) )
                    {
                        $class_object->__set( $key, $value );
                    }
                    else
                    {
                        $class_object->{$key} = $value;
                    }
                }

                $this->_result_into[ $index ] = $class_object;
            }

            return $this->_result_into;
        }
        elseif( class_exists( $type ) !== FALSE )
        {
            foreach( $this->_result_array as $index => $row )
            {
                $args = func_get_args();

                if( count( $args == 2 ) )
                {
                    $class_name = $args[ 0 ];
                    $class_object = new $class_name( $args[ 1 ] );
                }
                elseif( count( $args == 3 ) )
                {
                    $class_name = $args[ 0 ];
                    $class_object = new $class_name( $args[ 1 ], $args[ 2 ] );
                }
                elseif( count( $args == 4 ) )
                {
                    $class_name = $args[ 0 ];
                    $class_object = new $class_name( $args[ 1 ], $args[ 2 ], $args[ 3 ] );
                }
                elseif( count( $args == 5 ) )
                {
                    $class_name = $args[ 0 ];
                    $class_object = new $class_name( $args[ 1 ], $args[ 2 ], $args[ 3 ], $args[ 4 ] );
                }

                foreach( $row as $key => $value )
                {
                    if( method_exists( $class_object, '__set' ) )
                    {
                        $class_object->__set( $key, $value );
                    }
                    else
                    {
                        $class_object->{$key} = $value;
                    }
                }

                $this->_result_class[ $index ] = $class_object;
            }

            return $this->_result_class;
        }
        elseif( $type == 'array' )
        {
            return $this->_result_array;
        }
    }

    // ------------------------------------------------------------------------

    public function result_array()
    {
        return $this->_result_array;
    }

    public function result_object()
    {
        return $this->_result_object;
    }

    public function result_into()
    {
        return $this->_result_into;
    }

    /**
     * Rows
     *
     * Alias for Result Method
     *
     * @access  public
     * @return  mixed
     */
    public function rows( $type = NULL )
    {
        return $this->result( $type );
    }

    // ------------------------------------------------------------------------

    /**
     * Row
     *
     * Return single row of query result, by default it's returning the first row.
     *
     * @param   int $index Row Index
     *
     * @access  public
     * @return  mixed
     */
    public function row( $index = 1, $type = NULL )
    {
        $result = $this->result( $type );

        return isset( $result[ $index ] ) ? $result[ $index ] : NULL;
    }

    // ------------------------------------------------------------------------

    /**
     * First Row
     *
     * Return first row of query result.
     *
     * @access  public
     * @return  mixed
     */
    public function first_row( $type = NULL )
    {
        $result = $this->result( $type );

        return ( count( $result ) === 0 ) ? NULL : reset( $result );
    }

    // ------------------------------------------------------------------------

    /**
     * Last Row
     *
     * Return last row of query result.
     *
     * @access  public
     * @return  mixed
     */
    public function last_row( $type = NULL )
    {
        $result = $this->result( $type );

        return ( count( $result ) === 0 ) ? NULL : end( $result );
    }

    // ------------------------------------------------------------------------

    /**
     * Returns the "next" row
     *
     * @param    string $type
     *
     * @return    mixed
     */
    public function next_row( $type = 'object' )
    {
        $result = $this->result( $type );

        if( count( $result ) === 0 )
        {
            return NULL;
        }

        return isset( $result[ $this->_current_row + 1 ] )
            ? $result[ ++$this->_current_row ]
            : NULL;
    }

    // --------------------------------------------------------------------

    /**
     * Returns the "previous" row
     *
     * @param    string $type
     *
     * @return    mixed
     */
    public function previous_row( $type = 'object' )
    {
        $result = $this->result( $type );
        if( count( $result ) === 0 )
        {
            return NULL;
        }

        if( isset( $result[ $this->_current_row - 1 ] ) )
        {
            --$this->_current_row;
        }

        return $result[ $this->_current_row ];
    }

    // --------------------------------------------------------------------

    /**
     * Num Rows
     *
     * The number of rows returned by the query.
     *
     * @access  public
     * @return  int
     */
    public function num_rows()
    {
        return empty( $this->_result_array ) ? 0 : count( $this->_result_array );
    }

    // ------------------------------------------------------------------------

    /**
     * Num Fields
     *
     * The number of FIELDS (columns) returned by the query.
     *
     * @access  public
     * @return  bool|int
     */
    public function num_fields()
    {
        $row = $this->first_row( 'array' );

        if( ! empty( $row ) )
        {
            foreach( $row as $key => $value )
            {
                $fields[ ] = $key;
            }

            return count( $fields );
        }

        return 0;
    }

    // ------------------------------------------------------------------------

    /**
     * Free Result
     *
     * It frees the memory associated with the result and deletes the result resource ID.
     * Normally PHP frees its memory automatically at the end of script execution.
     * However, if you are running a lot of queries in a particular script you might want to free
     * the result after each query result has been generated in order to cut down on memory consumptions.
     *
     * @access  public
     */
    public function free_result()
    {
        $this->_query = NULL;
        $this->_current_row = 1;
        $this->_result_array = array();
        $this->_result_object = array();
    }
}