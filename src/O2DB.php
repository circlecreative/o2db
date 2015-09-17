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
 * @license        http://opensource.org/licenses/MIT	MIT License
 * @link           http://circle-creative.com/products/o2db.html
 * @filesource
 */

// ------------------------------------------------------------------------

namespace O2System;

// ------------------------------------------------------------------------

use O2System\O2Gears\Logger;

/**
 * O2DB Bootstrap
 *
 * Porting from CodeIgniter Database Libraries
 *
 * @category    Bootstrap Class
 * @author      Circle Creative Developer Team
 * @link        http://o2system.in/features/standalone/o2db.html
 */
class O2DB
{
    protected $_valid_drivers;

    protected $_config = array();
    protected $_conn;

    /**
     * Class Constructor
     *
     * @uses    \O2System\O2Gears\Logger::info();
     *
     * @access  public
     */
    public function __construct()
    {
        foreach( glob( SYSTEMPATH . 'libraries/database/drivers/*', GLOB_ONLYDIR ) as $package )
        {
            $package = str_replace( '\\', '/', $package );
            $package = explode( '/', $package );
            $this->_valid_drivers[ ] = end( $package );
        }

        Logger::info( 'Database (DB) Library Initialized' );
    }

    /**
     * Set Database Configurations
     *
     * @param array $config Database Configurations Array
     *
     * @access  public
     * @return  \O2System\O2DB
     */
    public function set_config(array $config = array())
    {
        $this->_config = $config;

        return $this;
    }

    /**
     * Connect to Database Engine
     *
     * @param string $conn  Connection Key Name or DSN Connection
     *
     * @access  public
     * @return  object  Returning Database Driver Class Object
     * @throws \Exception
     */
    public function &connect( $conn = NULL )
    {
        $conn = empty( $conn ) ? 'default' : $conn;

        // Load the DB config file if a DSN string wasn't passed
        if( isset( $this->_config[ $conn ] ) )
        {
            $this->_conn = $this->_config[ $conn ];
        }
        elseif( is_string( $conn ) && strpos( $conn, '://' ) !== FALSE )
        {
            /**
             * Parse the URL from the DSN string
             * Database settings can be passed as discreet
             * parameters or as a data source name in the first
             * parameter. DSNs must have this prototype:
             * $dsn = 'driver://username:password@hostname/database';
             */
            if( ( $dsn = @parse_url( $conn ) ) === FALSE )
            {
                throw new \Exception( 'Invalid DB Connection String' );
            }

            $this->_conn = array(
                'db_driver' => $dsn[ 'scheme' ],
                'hostname'  => isset( $dsn[ 'host' ] ) ? rawurldecode( $dsn[ 'host' ] ) : '',
                'port'      => isset( $dsn[ 'port' ] ) ? rawurldecode( $dsn[ 'port' ] ) : '',
                'username'  => isset( $dsn[ 'user' ] ) ? rawurldecode( $dsn[ 'user' ] ) : '',
                'password'  => isset( $dsn[ 'pass' ] ) ? rawurldecode( $dsn[ 'pass' ] ) : '',
                'database'  => isset( $dsn[ 'path' ] ) ? rawurldecode( substr( $dsn[ 'path' ], 1 ) ) : ''
            );

            // Were additional config items set?
            if( isset( $dsn[ 'query' ] ) )
            {
                parse_str( $dsn[ 'query' ], $extra );

                foreach( $extra as $key => $val )
                {
                    if( is_string( $val ) && in_array( strtoupper( $val ), array( 'TRUE', 'FALSE', 'NULL' ) ) )
                    {
                        $val = var_export( $val, TRUE );
                    }

                    static::$_conn[ $key ] = $val;
                }
            }
        }

        // No DB specified yet? Beat them senseless...
        if( empty( $this->_conn[ 'db_driver' ] ) )
        {
            throw new \Exception( 'You have not selected a database type to connect to.' );
        }

        if( ! in_array( $this->_conn[ 'db_driver' ], $this->_valid_drivers ) )
        {
            throw new \Exception( 'Unsupported database driver.' );
        }

        // Instantiate the DB adapter
        $db_driver_class = '\O2System\O2DB\Drivers\\' . ucfirst( $this->_conn[ 'db_driver' ] ) . '\Driver';
        $db_adapter = new $db_driver_class( $this->_conn );

        // Check for a sub_db_driver
        if( ! empty( $this->_config[ 'sub_db_driver' ] ) )
        {
            if( ! isset( $db_adapter->_valid_subdrivers[ $this->_config[ 'sub_db_driver' ] ] ) )
            {
                throw new \Exception( 'Unsupported database sub_db_driver.' );
            }

            $db_subdriver_class = '\O2System\O2DB\Drivers\\' . ucfirst( $this->_conn[ 'db_driver' ] ) . '\\' . ucfirst( $this->_config[ 'sub_db_driver' ] ) . '\Driver';
            $db_adapter = new $db_subdriver_class( static::$_conn );
        }

        $db_adapter->initialize();

        return $db_adapter;
    }
}
