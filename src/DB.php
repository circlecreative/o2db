<?php
/**
 * O2DB
 *
 * Open Source PHP Data Object Wrapper for PHP 5.4.0 or newer
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
 * @filesource
 */
// ------------------------------------------------------------------------

namespace O2System
{

	use O2System\Glob\Factory\Magics;

	class DB
	{
		use Magics;

		/**
		 * PDO Compatible Drivers List
		 *
		 * @access  protected
		 * @type    array
		 */
		protected $_valid_drivers = array(
			'cubrid'   => 'Cubrid',
			'mysql'    => 'MySQL',
			'mssql'    => 'MsSQL',
			'firebird' => 'Firebird',
			'ibm'      => 'IBM',
			'informix' => 'Informix',
			'oci'      => 'Oracle',
			'odbc'     => 'ODBC',
			'pgsql'    => 'PostgreSQL',
			'sqlite'   => 'SQLite',
		);
		
		protected $_config = array();
		
		public $forge   = FALSE;
		public $utility = FALSE;

		/**
		 * Class Constructor
		 *
		 * @param array $config
		 *
		 * @access  public
		 * @throws  DB\Exception
		 */
		public function __construct( $config )
		{
			if ( is_string( $config ) AND strpos( $config, '://' ) !== FALSE )
			{
				/**
				 * Parse the URL from the DSN string
				 * Database settings can be passed as discreet
				 * parameters or as a data source name in the first
				 * parameter. DSNs must have this prototype:
				 * $dsn = 'driver://username:password@hostname/database';
				 */
				if ( ( $dsn = @parse_url( $config ) ) === FALSE )
				{
					throw new DB\Exception( 'Invalid DB Connection String' );
				}

				$config = array(
					'driver'   => $dsn[ 'scheme' ],
					'hostname' => isset( $dsn[ 'host' ] ) ? rawurldecode( $dsn[ 'host' ] ) : '',
					'port'     => isset( $dsn[ 'port' ] ) ? rawurldecode( $dsn[ 'port' ] ) : '',
					'username' => isset( $dsn[ 'user' ] ) ? rawurldecode( $dsn[ 'user' ] ) : '',
					'password' => isset( $dsn[ 'pass' ] ) ? rawurldecode( $dsn[ 'pass' ] ) : '',
					'database' => isset( $dsn[ 'path' ] ) ? rawurldecode( substr( $dsn[ 'path' ], 1 ) ) : '',
				);

				// Validate Connection
				$config[ 'username' ] = $config[ 'username' ] === 'username' ? NULL : $config[ 'username' ];
				$config[ 'password' ] = $config[ 'password' ] === 'password' ? NULL : $config[ 'password' ];
				$config[ 'hostname' ] = $config[ 'hostname' ] === 'hostname' ? NULL : $config[ 'hostname' ];

				// Were additional config items set?
				if ( isset( $dsn[ 'query' ] ) )
				{
					parse_str( $dsn[ 'query' ], $extra );

					foreach ( $extra as $key => $value )
					{
						if ( is_string( $value ) AND in_array( strtoupper( $value ), array( 'TRUE', 'FALSE', 'NULL' ) ) )
						{
							$value = var_export( $value, TRUE );
						}

						$config[ $key ] = $value;
					}
				}
			}

			if ( empty( $config[ 'driver' ] ) )
			{
				throw new DB\Exception( 'You have not selected a database type to connect to.' );
			}

			if ( in_array( $config[ 'driver' ], array( 'mssql', 'sybase' ) ) )
			{
				$config[ 'driver' ] = 'dblib';
			}

			if ( ! array_key_exists( $config[ 'driver' ], $this->_valid_drivers ) )
			{
				throw new DB\Exception( 'Unsupported database driver.' );
			}

			if ( is_dir( $driver_path = __DIR__ . '/Drivers/' . ucfirst( $config[ 'driver' ] . '/' ) ) )
			{
				// Create DB Connection
				$class_name = '\O2System\DB\Drivers\\' . ucfirst( $config[ 'driver' ] ) . '\\Driver';

				// Create Instance
				static::$_instance = new $class_name( $config );
				static::$_instance->connect();
				
				if ( static::$_instance->is_connected() === TRUE )
				{
					$this->_config = $config;
					
					// Create Glob Magic
					$this->_reflection( $class_name );
				}
			}
		}

		// ------------------------------------------------------------------------
		
		public function load( $tool )
		{
			switch ( $tool )
			{
				case 'forge':
					
					$forge_class_name = '\O2System\DB\Drivers\\' . ucfirst( $this->_config[ 'driver' ] ) . '\\Forge';
					$this->forge = new $forge_class_name( static::$_instance );
					
					return $this->forge;
					break;
				case 'utility':
					
					$utility_class_name = '\O2System\DB\Drivers\\' . ucfirst( $this->_config[ 'driver' ] ) . '\\Utility';
					$this->utility = new $utility_class_name( static::$_instance );
					
					return $this->utility;
					break;
			}
			
			return FALSE;
		}

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

	use O2System\Glob\Exception\Interfaces as ExceptionInterface;

	class Exception extends ExceptionInterface
	{
		protected $_sql = NULL;

		public function __construct( $message = NULL, $code = 0, $sql = NULL )
		{
			if ( $message instanceof \PDOException OR
				$message instanceof \Exception
			)
			{
				$this->code = $message->getCode();
				$this->message = $message->getMessage();
			}

			if ( isset( $sql ) )
			{
				$this->_sql = $sql;
			}

			// Now to correct the code number.
			$state = $this->getMessage();

			if ( ! strstr( $state, 'SQLSTATE[' ) )
			{
				$state = $this->getCode();
			}

			if ( strstr( $state, 'SQLSTATE[' ) )
			{
				preg_match( '/SQLSTATE\[(\w+)\] \[(\w+)\] (.*)/', $state, $matches );
				$this->code = ( $matches[ 1 ] == 'HT000' ? $matches[ 2 ] : $matches[ 1 ] );
				$this->message = $matches[ 3 ];
			}

			// Let PDOException do its normal thing
			parent::__construct( $message, $code );

			// Register Custom Exception View Path
			$this->register_view_paths( __DIR__ . '/Views/' );
		}

		public function getSql()
		{
			return $this->_sql;
		}
	}
}