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

use O2System\O2DB\Interfaces\Driver as DriverInterface;

/**
 * PDO Database Driver
 *
 * @author      Circle Creative Developer Team
 */
class Driver extends DriverInterface
{
    /**
     * PDO Options
     *
     * @access  public
     * @type    array
     */
    public $options = array();

    // --------------------------------------------------------------------

    /**
     * Class constructor
     *
     * Validates the DSN string and/or detects the sub_driver.
     *
     * @param   array $params
     *
     * @access  public
     * @throws  \Exception
     */
    public function __construct( $params )
    {
        parent::__construct( $params );

        if( preg_match( '/([^:]+):/', $this->dsn, $match ) && count( $match ) === 2 )
        {
            // If there is a minimum valid dsn string pattern found, we're done
            // This is for general PDO users, who tend to have a full DSN string.
            $this->sub_driver = $match[ 1 ];

            return;
        }
        // Legacy support for DSN specified in the hostname field
        elseif( preg_match( '/([^:]+):/', $this->hostname, $match ) && count( $match ) === 2 )
        {
            $this->dsn = $this->hostname;
            $this->hostname = NULL;
            $this->sub_driver = $match[ 1 ];

            return;
        }
        elseif( in_array( $this->sub_driver, array( 'mssql', 'sybase' ), TRUE ) )
        {
            $this->sub_driver = 'dblib';
        }
        elseif( $this->sub_driver === '4D' )
        {
            $this->sub_driver = '4d';
        }
        elseif( ! in_array( $this->sub_driver, array(
            '4d', 'cubrid', 'dblib', 'firebird', 'ibm', 'informix', 'mysql', 'oci', 'odbc', 'pgsql', 'sqlite', 'sqlsrv'
        ), TRUE )
        )
        {
            if( $this->debug_mode )
            {
                throw new \Exception( 'PDO: Invalid or non-existent DB Driver' );
            }
        }

        $this->dsn = NULL;
    }

    // --------------------------------------------------------------------

    /**
     * Database connection
     *
     * @param   bool $persistent
     *
     * @access  public
     * @return  object
     * @throws  \Exception
     */
    public function connect( $persistent = FALSE )
    {
        $this->options[ \PDO::ATTR_PERSISTENT ] = $persistent;

        try
        {
            return new \PDO( $this->dsn, $this->username, $this->password, $this->options );
        }
        catch( \PDOException $e )
        {
            if( $this->debug_mode && empty( $this->failover ) )
            {
                throw new \Exception( $e->getMessage() );
            }

            return FALSE;
        }
    }

    // --------------------------------------------------------------------

    /**
     * Database version number
     *
     * @access  public
     * @return  string
     */
    public function version()
    {
        if( isset( $this->data_cache[ 'version' ] ) )
        {
            return $this->data_cache[ 'version' ];
        }

        // Not all subdrivers support the getAttribute() method
        try
        {
            return $this->data_cache[ 'version' ] = $this->id_connection->getAttribute( \PDO::ATTR_SERVER_VERSION );
        }
        catch( \PDOException $e )
        {
            return parent::version();
        }
    }

    // --------------------------------------------------------------------

    /**
     * Begin Transaction
     *
     * @param   bool $test_mode
     *
     * @access  public
     * @return  bool
     */
    public function trans_begin( $test_mode = FALSE )
    {
        // When transactions are nested we only begin/commit/rollback the outermost ones
        if( ! $this->trans_enabled OR $this->_trans_depth > 0 )
        {
            return TRUE;
        }

        // Reset the transaction failure flag.
        // If the $test_mode flag is set to TRUE transactions will be rolled back
        // even if the queries produce a successful result.
        $this->_trans_failure = ( $test_mode === TRUE );

        return $this->id_connection->beginTransaction();
    }

    // --------------------------------------------------------------------

    /**
     * Commit Transaction
     *
     * @access  public
     * @return  bool
     */
    public function trans_commit()
    {
        // When transactions are nested we only begin/commit/rollback the outermost ones
        if( ! $this->trans_enabled OR $this->_trans_depth > 0 )
        {
            return TRUE;
        }

        return $this->id_connection->commit();
    }

    // --------------------------------------------------------------------

    /**
     * Rollback Transaction
     *
     * @access  public
     * @return  bool
     */
    public function trans_rollback()
    {
        // When transactions are nested we only begin/commit/rollback the outermost ones
        if( ! $this->trans_enabled OR $this->_trans_depth > 0 )
        {
            return TRUE;
        }

        return $this->id_connection->rollBack();
    }

    // --------------------------------------------------------------------

    /**
     * Affected Rows
     *
     * @access  public
     * @return  int
     */
    public function affected_rows()
    {
        return is_object( $this->id_result ) ? $this->id_result->rowCount() : 0;
    }

    // --------------------------------------------------------------------

    /**
     * Insert ID
     *
     * @param   string $name
     *
     * @access  public
     * @return  int
     */
    public function insert_id( $name = NULL )
    {
        return $this->id_connection->lastInsertId( $name );
    }

    // --------------------------------------------------------------------

    /**
     * Error
     *
     * Returns an array containing code and message of the last
     * database error that has occured.
     *
     * @access  public
     * @return  array
     */
    public function error()
    {
        $error = array( 'code' => '00000', 'message' => '' );
        $pdo_error = $this->id_connection->errorInfo();

        if( empty( $pdo_error[ 0 ] ) )
        {
            return $error;
        }

        $error[ 'code' ] = isset( $pdo_error[ 1 ] ) ? $pdo_error[ 0 ] . '/' . $pdo_error[ 1 ] : $pdo_error[ 0 ];
        if( isset( $pdo_error[ 2 ] ) )
        {
            $error[ 'message' ] = $pdo_error[ 2 ];
        }

        return $error;
    }

    // --------------------------------------------------------------------

    /**
     * Execute the query
     *
     * @param   string $sql SQL query
     *
     * @access  public
     * @return  mixed
     */
    protected function _execute( $sql )
    {
        return $this->id_connection->query( $sql );
    }

    // --------------------------------------------------------------------

    /**
     * Platform-dependant string escape
     *
     * @param   string
     *
     * @access  protected
     * @return  string
     */
    protected function _escape_string( $string )
    {
        // Escape the string
        $string = $this->id_connection->quote( $string );

        // If there are duplicated quotes, trim them away
        return ( $string[ 0 ] === "'" )
            ? substr( $string, 1, -1 )
            : $string;
    }

    // --------------------------------------------------------------------

    /**
     * Field data query
     *
     * Generates a platform-specific query so that the column data can be retrieved
     *
     * @param   string $table
     *
     * @access  protected
     * @return  string
     */
    protected function _field_data( $table )
    {
        return 'SELECT TOP 1 * FROM ' . $this->protect_identifiers( $table );
    }

    // --------------------------------------------------------------------

    /**
     * Update_Batch statement
     *
     * Generates a platform-specific batch update string from the supplied data
     *
     * @param   string $table  Table name
     * @param   array  $values Update data
     * @param   string $index  WHERE key
     *
     * @access  protected
     * @return  string
     */
    protected function _update_batch( $table, $values, $index )
    {
        $ids = array();
        foreach( $values as $key => $val )
        {
            $ids[ ] = $val[ $index ];

            foreach( array_keys( $val ) as $field )
            {
                if( $field !== $index )
                {
                    $final[ $field ][ ] = 'WHEN ' . $index . ' = ' . $val[ $index ] . ' THEN ' . $val[ $field ];
                }
            }
        }

        $cases = '';
        foreach( $final as $k => $v )
        {
            $cases .= $k . ' = CASE ' . "\n";

            foreach( $v as $row )
            {
                $cases .= $row . "\n";
            }

            $cases .= 'ELSE ' . $k . ' END, ';
        }

        $this->where( $index . ' IN(' . implode( ',', $ids ) . ')', NULL, FALSE );

        return 'UPDATE ' . $table . ' SET ' . substr( $cases, 0, -2 ) . $this->_compile_where( '_where' );
    }

    // --------------------------------------------------------------------

    /**
     * Truncate statement
     *
     * Generates a platform-specific truncate string from the supplied data
     *
     * If the database does not support the TRUNCATE statement,
     * then this method maps to 'DELETE FROM table'
     *
     * @param   string $table
     *
     * @access  protected
     * @return  string
     */
    protected function _truncate( $table )
    {
        return 'TRUNCATE TABLE ' . $table;
    }

}
