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

use O2System\Core\Exception;
use O2System\Core\Gears\Logger;

defined( 'BASEPATH' ) OR exit( 'No direct script access allowed' );


class Driver extends \O2System\O2DB
{

    /**
     * Database driver
     *
     * @var    string
     */
    public $dbdriver = 'pdo';

    /**
     * PDO Options
     *
     * @var    array
     */
    public $options = array();

    // --------------------------------------------------------------------

    /**
     * Class constructor
     *
     * Validates the DSN string and/or detects the sub_db_driver.
     *
     * @access public
     *
     * @param    array $params
     *
     * @return    void
     */
    public function __construct( $params )
    {
        parent::__construct( $params );

        if( preg_match( '/([^:]+):/', $this->dsn, $match ) && count( $match ) === 2 )
        {
            // If there is a minimum valid dsn string pattern found, we're done
            // This is for general PDO users, who tend to have a full DSN string.
            $this->sub_db_driver = $match[ 1 ];

            return;
        }
        // Legacy support for DSN specified in the hostname field
        elseif( preg_match( '/([^:]+):/', $this->hostname, $match ) && count( $match ) === 2 )
        {
            $this->dsn = $this->hostname;
            $this->hostname = NULL;
            $this->sub_db_driver = $match[ 1 ];

            return;
        }
        elseif( in_array( $this->sub_db_driver, array( 'mssql', 'sybase' ), TRUE ) )
        {
            $this->sub_db_driver = 'dblib';
        }
        elseif( $this->sub_db_driver === '4D' )
        {
            $this->sub_db_driver = '4d';
        }
        elseif( ! in_array( $this->sub_db_driver, array(
            '4d', 'cubrid', 'dblib', 'firebird', 'ibm', 'informix', 'mysql', 'oci', 'odbc', 'pgsql', 'sqlite', 'sqlsrv'
        ), TRUE )
        )
        {
            Logger::error( 'PDO: Invalid or non-existent sub_db_driver' );
            //log_message('error', 'PDO: Invalid or non-existent sub_db_driver');

            if( $this->db_debug )
            {
                Exception::show( 'Invalid or non-existent PDO sub_db_driver' );
                //show_error('Invalid or non-existent PDO sub_db_driver');
            }
        }

        $this->dsn = NULL;
    }

    // --------------------------------------------------------------------

    /**
     * Database connection
     *
     * @access public
     *
     * @param    bool $persistent
     *
     * @return    object
     */
    public function db_connect( $persistent = FALSE )
    {
        $this->options[ \PDO::ATTR_PERSISTENT ] = $persistent;

        try
        {
            return new \PDO( $this->dsn, $this->username, $this->password, $this->options );
        }
        catch( \PDOException $e )
        {
            if( $this->db_debug && empty( $this->failover ) )
            {
                $this->display_error( $e->getMessage(), '', TRUE );
            }

            return FALSE;
        }
    }

    // --------------------------------------------------------------------

    /**
     * Database version number
     *
     * @access public
     *
     * @return    string
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
            return $this->data_cache[ 'version' ] = $this->conn_id->getAttribute( \PDO::ATTR_SERVER_VERSION );
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
     * @access public
     *
     * @param    bool $test_mode
     *
     * @return    bool
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

        return $this->conn_id->beginTransaction();
    }

    // --------------------------------------------------------------------

    /**
     * Commit Transaction
     *
     * @access public
     *
     * @return    bool
     */
    public function trans_commit()
    {
        // When transactions are nested we only begin/commit/rollback the outermost ones
        if( ! $this->trans_enabled OR $this->_trans_depth > 0 )
        {
            return TRUE;
        }

        return $this->conn_id->commit();
    }

    // --------------------------------------------------------------------

    /**
     * Rollback Transaction
     *
     * @access public
     *
     * @return    bool
     */
    public function trans_rollback()
    {
        // When transactions are nested we only begin/commit/rollback the outermost ones
        if( ! $this->trans_enabled OR $this->_trans_depth > 0 )
        {
            return TRUE;
        }

        return $this->conn_id->rollBack();
    }

    // --------------------------------------------------------------------

    /**
     * Affected Rows
     *
     * @access public
     *
     * @return    int
     */
    public function affected_rows()
    {
        return is_object( $this->result_id ) ? $this->result_id->rowCount() : 0;
    }

    // --------------------------------------------------------------------

    /**
     * Insert ID
     *
     * @access public
     *
     * @param    string $name
     *
     * @return    int
     */
    public function insert_id( $name = NULL )
    {
        return $this->conn_id->lastInsertId( $name );
    }

    // --------------------------------------------------------------------

    /**
     * Error
     *
     * Returns an array containing code and message of the last
     * database error that has occured.
     *
     * @access public
     *
     * @return    array
     */
    public function error()
    {
        $error = array( 'code' => '00000', 'message' => '' );
        $pdo_error = $this->conn_id->errorInfo();

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
     * @access public
     *
     * @param    string $sql SQL query
     *
     * @return    mixed
     */
    protected function _execute( $sql )
    {
        return $this->conn_id->query( $sql );
    }

    // --------------------------------------------------------------------

    /**
     * Platform-dependant string escape
     *
     * @access protected
     *
     * @param    string
     *
     * @return    string
     */
    protected function _escape_str( $str )
    {
        // Escape the string
        $str = $this->conn_id->quote( $str );

        // If there are duplicated quotes, trim them away
        return ( $str[ 0 ] === "'" )
            ? substr( $str, 1, -1 )
            : $str;
    }

    // --------------------------------------------------------------------

    /**
     * Field data query
     *
     * Generates a platform-specific query so that the column data can be retrieved
     *
     * @access protected
     *
     * @param    string $table
     *
     * @return    string
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
     * @access protected
     *
     * @param    string $table Table name
     * @param    array  $values Update data
     * @param    string $index WHERE key
     *
     * @return    string
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

        return 'UPDATE ' . $table . ' SET ' . substr( $cases, 0, -2 ) . $this->_compile_wh( 'qb_where' );
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
     * @access protected
     *
     * @param    string $table
     *
     * @return    string
     */
    protected function _truncate( $table )
    {
        return 'TRUNCATE TABLE ' . $table;
    }

}

/* End of file Driver.php */
/* Location: ./o2system/libraries/database/drivers/PDO/Driver.php */
