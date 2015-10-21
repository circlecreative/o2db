<?php
/**
 * O2DB
 *
 * An open source PHP database engine driver for PHP 5.4 or newer
 *
 * This content is released under the MIT License (MIT)
 *
 * Copyright (c) 2015, PT. Lingkar Kreasi (Circle Creative).
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

namespace O2System
{
    use O2System\DB\Exception;

    /**
     * O2DB Bootstrap
     *
     * @category    Bootstrap Class
     * @author      Circle Creative Developer Team
     * @link        http://o2system.in/features/standalone/o2db.html
     */
    class DB
    {
        /**
         * PDO Compatible Drivers List
         *
         * @access  protected
         * @type    array
         */
        protected $_valid_drivers = array(
            //'cubrid'     => 'Cubrid',
            'mysql' => 'MySQL',
            //'mssql'      => 'MsSQL',
            //'firebird'   => 'Firebird',
            //'ibm'        => 'IBM',
            //'informix'   => 'Informix',
            //'oracle'     => 'Oracle',
            //'odbc'       => 'ODBC',
            //'postgresql' => 'PostgreSQL',
            'sqlite' => 'SQLite',
            //'4d'         => '4D'
        );

        /**
         * Database Class Configuration
         *
         * @access  protected
         * @type    array
         */
        protected static $_config;

        /**
         * Active connection
         *
         * @access  protected
         * @type    \O2System\DB\Interfaces\Connection
         */
        protected $_conn;

        /**
         * Class Constructor
         *
         * @param array $config
         *
         * @access  public
         */
        public function __construct( array $config = array() )
        {
            if( ! empty( $config ) )
            {
                if(isset($config['default']))
                {
                    if(! empty($config['default']['dsn']) OR
                       (! empty($config['default']['username']) AND ! empty($config['default']['password']))
                    )
                    {
                        static::$_config = $config;
                    }
                }
            }

            set_exception_handler( '\O2System\DB\Exception::exception_handler' );
        }

        // ------------------------------------------------------------------------

        /**
         * Connect
         *
         * Connect to database using config or dsn
         *
         * @param   string $connection
         *
         * @access  public
         * @return  bool|DB\Interfaces\Connection
         * @throws  DB::Error
         */
        public function connect( $connection = NULL )
        {
            $connection = empty( $connection ) ? 'default' : $connection;

            // Load the DB config file if a DSN string wasn't passed
            if( isset( static::$_config[ $connection ] ) )
            {
                $connection = static::$_config[ $connection ];
            }
            elseif( is_string( $connection ) && strpos( $connection, '://' ) !== FALSE )
            {
                /**
                 * Parse the URL from the DSN string
                 * Database settings can be passed as discreet
                 * parameters or as a data source name in the first
                 * parameter. DSNs must have this prototype:
                 * $dsn = 'driver://username:password@hostname/database';
                 */
                if( ( $dsn = @parse_url( $connection ) ) === FALSE )
                {
                    throw new Exception( 'Invalid DB Connection String', 100 );
                }

                $connection = array(
                    'driver'   => $dsn[ 'scheme' ],
                    'hostname' => isset( $dsn[ 'host' ] ) ? rawurldecode( $dsn[ 'host' ] ) : '',
                    'port'     => isset( $dsn[ 'port' ] ) ? rawurldecode( $dsn[ 'port' ] ) : '',
                    'username' => isset( $dsn[ 'user' ] ) ? rawurldecode( $dsn[ 'user' ] ) : '',
                    'password' => isset( $dsn[ 'pass' ] ) ? rawurldecode( $dsn[ 'pass' ] ) : '',
                    'database' => isset( $dsn[ 'path' ] ) ? rawurldecode( substr( $dsn[ 'path' ], 1 ) ) : ''
                );

                // Validate Connection
                $connection[ 'username' ] = $connection[ 'username' ] === 'username' ? NULL : $connection[ 'username' ];
                $connection[ 'password' ] = $connection[ 'password' ] === 'password' ? NULL : $connection[ 'password' ];
                $connection[ 'hostname' ] = $connection[ 'hostname' ] === 'hostname' ? NULL : $connection[ 'hostname' ];

                // Were additional config items set?
                if( isset( $dsn[ 'query' ] ) )
                {
                    parse_str( $dsn[ 'query' ], $extra );

                    foreach( $extra as $key => $value )
                    {
                        if( is_string( $value ) AND in_array( strtoupper( $value ), array( 'TRUE', 'FALSE', 'NULL' ) ) )
                        {
                            $value = var_export( $value, TRUE );
                        }

                        $connection[ $key ] = $value;
                    }
                }
            }
            else
            {
                throw new Exception( 'Required DB Connection Configuration', 101 );
            }

            if( empty( $connection[ 'driver' ] ) )
            {
                throw new Exception( 'You have not selected a database type to connect to.', 102 );
            }

            if( ! array_key_exists( $connection[ 'driver' ], $this->_valid_drivers ) )
            {
                throw new Exception( 'Unsupported database driver.', 103 );
            }

            if( is_dir( $driver_path = __DIR__ . '/Drivers/' . ucfirst( $connection[ 'driver' ] . '/' ) ) )
            {
                // Create DB Connection
                $class_name = '\O2System\DB\Drivers\\' . ucfirst( $connection[ 'driver' ] ) . '\\Driver';
                $this->_conn = new $class_name( $connection );

                return $this->_conn;
            }

            return FALSE;
        }

        // ------------------------------------------------------------------------

        /**
         * Server supported drivers
         *
         * @access public
         * @return array
         */
        public static function get_supported_drivers()
        {
            return \PDO::getAvailableDrivers();
        }
    }
}

namespace O2System\DB
{
    use O2System\Gears\Tracer;

    class Exception extends \PDOException
    {
        protected $statement = NULL;

        public function __construct( $message = NULL, $code = 0, \PDOException $previous = NULL )
        {
            // in case they call: new MyException($somePDOException);
            // instead of following the interface.
            //
            if( $message instanceof \PDOException )
            {
                $previous = $message;
                $code = $previous->getCode();
                $message = $previous->getMessage();
            }

            // Let PDOException do its normal thing
            //
            parent::__construct( $message, $code, $previous );

            // Now to correct the code number.
            $state = $this->getMessage();
            if( ! strstr( $state, 'SQLSTATE[' ) )
            {
                $state = $this->getCode();
            }
            if( strstr( $state, 'SQLSTATE[' ) )
            {
                preg_match( '/SQLSTATE\[(\w+)\] \[(\w+)\] (.*)/', $state, $matches );
                $this->code = ( $matches[ 1 ] == 'HT000' ? $matches[ 2 ] : $matches[ 1 ] );
                $this->message = $matches[ 3 ];
            }
        }

        public function setStatement($sql)
        {
            $this->statement = $sql;
        }

        public function getStatement()
        {
            return $this->statement;
        }

        public static function exception_handler( $exception )
        {
            $tracer = new Tracer( (array)$exception->getTrace() );

            if( PHP_SAPI === 'cli' )
            {
                $template = __DIR__ . '/Views/cli_exception.php';
            }
            else
            {
                $template = __DIR__ . '/Views/html_exception.php';
            }

            if( ob_get_level() > 1 )
            {
                ob_end_flush();
            }

            header( 'HTTP/1.1 500 Internal Server Error', TRUE, 500 );

            ob_start();
            include( $template );
            $buffer = ob_get_contents();
            ob_end_clean();
            echo $buffer;

            exit( 1 );
        }
    }
}
