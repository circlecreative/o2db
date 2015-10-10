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

namespace O2System\DB\Drivers\Mysql;

// ------------------------------------------------------------------------

use O2System\DB\Interfaces\Driver as DriverInterface;

/**
 * MySQL Driver Connection Class
 *
 * @package     O2DB
 * @subpackage  Drivers/Mysql
 * @category    Driver Class
 * @author      Circle Creative Developer Team
 * @link        http://circle-creative.com/products/o2db.html
 */
class Driver extends DriverInterface
{
    /**
     * Database Engine
     *
     * @access  public
     * @type    string
     */
    public $platform = 'MySQL';

    /**
     * Database Port
     *
     * @access  public
     * @type    string
     */
    public $port = 3306;

    /**
     * Database Charset
     *
     * @access  public
     * @type    string
     */
    public $charset = 'utf8';

    /**
     * Database Collation
     *
     * @access  public
     * @type    string
     */
    public $collate = 'utf8_unicode_ci';

    /**
     * Default PDO Connection Options
     *
     * @access public
     * @var array
     */
    public $options = array(
        \PDO::ATTR_CASE                     => \PDO::CASE_NATURAL,
        \PDO::ATTR_ERRMODE                  => \PDO::ERRMODE_EXCEPTION,
        \PDO::ATTR_STRINGIFY_FETCHES        => FALSE,
        \PDO::ATTR_EMULATE_PREPARES         => FALSE,
        \PDO::MYSQL_ATTR_USE_BUFFERED_QUERY => FALSE
    );

    // ------------------------------------------------------------------------

    /**
     * Create DSN
     *
     * Create DSN Connection String
     *
     * @access  protected
     * @return  void
     */
    protected function _create_dsn()
    {
        $this->dsn = 'mysql:host=' . ( empty( $this->hostname ) ? '127.0.0.1' : $this->hostname );

        empty( $this->port ) OR $this->dsn .= ';port=' . $this->port;
        empty( $this->database ) OR $this->dsn .= ';dbname=' . $this->database;
        empty( $this->charset ) OR $this->dsn .= ';charset=' . $this->charset;

        /* Prior to PHP 5.3.6, even if the charset was supplied in the DSN
         * on connect - it was ignored. This is a work-around for the issue.
         *
         * Reference: http://www.php.net/manual/en/ref.pdo-mysql.connection.php
         */
        if( ! is_php( '5.3.6' ) && ! empty( $this->charset ) )
        {
            $this->options[ \PDO::MYSQL_ATTR_INIT_COMMAND ] = 'SET NAMES ' . $this->charset
                                                              . ( empty( $this->collate ) ? '' : ' COLLATE ' . $this->collate );
        }

        if( $this->strict_on )
        {
            if( empty( $this->options[ \PDO::MYSQL_ATTR_INIT_COMMAND ] ) )
            {
                $this->options[ \PDO::MYSQL_ATTR_INIT_COMMAND ] = 'SET SESSION sql_mode="STRICT_ALL_TABLES"';
            }
            else
            {
                $this->options[ \PDO::MYSQL_ATTR_INIT_COMMAND ] .= ', @@session.sql_mode = "STRICT_ALL_TABLES"';
            }
        }

        if( $this->compress === TRUE )
        {
            $this->options[ \PDO::MYSQL_ATTR_COMPRESS ] = TRUE;
        }

        if( $this->buffered === TRUE )
        {
            $this->options[ \PDO::MYSQL_ATTR_USE_BUFFERED_QUERY ] = TRUE;
        }

        if( $this->persistent === TRUE )
        {
            $this->options[ \PDO::ATTR_PERSISTENT ] = TRUE;
        }
    }

    // ------------------------------------------------------------------------

    /**
     * List Database
     *
     * Returns an array of database names
     *
     * @access  public
     * @return  array
     */
    public function list_database()
    {
        if( ! isset( static::$_registry[ 'databases' ] ) )
        {
            $query = $this->query( "SHOW DATABASES" );

            if( $query->num_rows() > 0 )
            {
                foreach( $query->result( 'array' ) as $row )
                {
                    static::$_registry[ 'databases' ][ ] = @reset( array_values( $row ) );
                }
            }
        }

        return static::$_registry[ 'databases' ];
    }

    // ------------------------------------------------------------------------

    /**
     * List Tables
     *
     * Returns an array of table names
     *
     * @access  public
     * @return  array
     */
    public function list_tables()
    {
        if( ! isset( static::$_registry[ 'tables' ] ) )
        {
            $query = $this->query( "SHOW TABLES" );

            if( $query->num_rows() > 0 )
            {
                foreach( $query->result( 'array' ) as $meta )
                {
                    $table = new \stdClass();
                    $table->name = @reset( array_values( $meta ) );

                    $metadata = $this->query( 'SHOW TABLE STATUS LIKE ' . $this->escape_string( $table->name ) );

                    if( $metadata->num_rows() == 1 )
                    {
                        $meta = $metadata->first_row();

                        $table->engine = $meta->Engine;
                        $table->auto_increment = $meta->Auto_increment;
                        $table->timestamp_create = $meta->Create_time;
                        $table->collation = $meta->Collation;
                        $table->comment = $meta->Comment;
                    }

                    $fields = $this->query( 'DESCRIBE ' . $this->escape_identifier( $table->name ) );

                    if( $fields->num_rows() > 0 )
                    {
                        $table->num_fields = 0;
                        foreach( $fields->result() as $meta )
                        {
                            $table->num_fields++;

                            $field = new \stdClass();
                            $field->name = $meta->Field;

                            // int(10) unsigned
                            preg_match_all( '/[\w]+/', $meta->Type, $x_type );
                            $field->type = strtoupper( $x_type[ 0 ][ 0 ] );
                            $field->length = isset( $x_type[ 0 ][ 1 ] ) ? $x_type[ 0 ][ 1 ] : NULL;

                            $field->not_null = $meta->Null === 'NO' ? TRUE : FALSE;
                            $field->primary_key = FALSE;

                            if( $meta->Key === 'PRI' )
                            {
                                $field->primary_key = TRUE;
                                $table->primary_keys[ ] = $field->name;
                            }

                            $field->default = $meta->Default;

                            if( $field->type === 'INT' AND isset( $x_type[ 0 ][ 2 ] ) ) ;
                            {
                                $field->unsigned = TRUE;
                            }

                            if( $meta->Extra === 'auto_increment' )
                            {
                                $field->auto_increment = TRUE;
                            }

                            $table->fields[ $field->name ] = $field;

                            unset( $x_type );
                        }
                    }

                    $indexes = $this->query( 'SHOW INDEXES FROM ' . $this->escape_identifier( $table->name ) );

                    if( $indexes->num_rows() > 0 )
                    {
                        foreach( $indexes->result() as $meta )
                        {
                            $index = new \stdClass();
                            $index->name = $meta->Column_name;
                            $index->unique = $meta->Non_unique == 1 ? TRUE : FALSE;
                            $index->type = $meta->Index_type;
                            $index->comment = $meta->Comment;

                            $table->indexes[ $meta->Column_name ] = $index;

                            if( ! empty( $table->fields ) )
                            {
                                if( array_key_exists( $meta->Column_name, $table->fields ) )
                                {
                                    $table->fields[ $meta->Column_name ]->indexes = TRUE;
                                }
                            }
                        }
                    }

                    static::$_registry[ 'tables' ][ $table->name ] = $table;
                }
            }
        }

        if( ! empty( static::$_registry[ 'tables' ] ) )
        {
            return array_keys( static::$_registry[ 'tables' ] );
        }
        else
        {
            return array();
        }
    }

}