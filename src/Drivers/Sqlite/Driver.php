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

namespace O2System\DB\Drivers\Sqlite;

// ------------------------------------------------------------------------

use O2System\DB\Interfaces\Driver as DriverInterface;

/**
 * SQLite Driver Connection Class
 *
 * @package     O2DB
 * @subpackage  Drivers/Sqlite
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
    public $platform = 'SQLite';

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
        if( empty( $this->database ) )
        {
            $this->dsn = 'sqlite::memory:';
        }
        else
        {
            if( file_exists( $this->database ) )
            {
                $this->dsn = 'sqlite:' . $this->database;
            }
            else
            {
                $this->dsn = 'sqlite::memory:';
            }
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
        if( strpos( $this->dsn, '::memory:' ) )
        {
            // not supported
            return array();
        }
        else
        {
            if( ! isset( static::$_registry[ 'databases' ] ) )
            {
                $path = pathinfo( $this->database, PATHINFO_DIRNAME ) . '/';
                $dbs = glob( $path . '*.db' );

                foreach( $dbs as $db )
                {
                    static::$_registry[ 'databases' ][ ] = pathinfo( $db, PATHINFO_FILENAME );
                }
            }

            return static::$_registry[ 'databases' ];
        }
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
            $query = $this->query( 'SELECT * FROM sqlite_master WHERE type = '.$this->escape_string('table') );

            if( $query->num_rows() > 0 )
            {
                foreach( $query->result() as $meta )
                {
                    $table = new \stdClass();
                    $table->name = $meta->name;
                    $table->engine = NULL;
                    $table->auto_increment = NULL;
                    $table->timestamp_create = NULL;
                    $table->collation = NULL;
                    $table->comment = NULL;

                    $fields = $this->query( 'PRAGMA TABLE_INFO(' . $this->escape_identifier( $table->name ) . ')' );

                    if( $fields->num_rows() > 0 )
                    {
                        $table->num_fields = 0;
                        foreach( $fields->result() as $meta )
                        {
                            $table->num_fields++;

                            $field = new \stdClass();
                            $field->name = $meta->name;

                            preg_match_all( '/[\w]+/', $meta->type, $x_type );
                            $field->type = isset( $x_type[ 0 ][ 0 ] ) ? strtoupper( $x_type[ 0 ][ 0 ] ) : $meta->type;
                            $field->length = isset( $x_type[ 0 ][ 1 ] ) ? $x_type[ 0 ][ 1 ] : NULL;

                            $field->not_null = $meta->notnull === '1' ? TRUE : FALSE;
                            $field->primary_key = FALSE;

                            if( $meta->pk === '1' )
                            {
                                $field->primary_key = TRUE;
                                $table->primary_keys[ ] = $field->name;
                            }

                            $field->default = $meta->dflt_value;

                            if( $field->type === 'INTEGER' AND $field->primary_key === TRUE ) ;
                            {
                                $field->auto_increment = TRUE;
                            }

                            $table->fields[ $field->name ] = $field;
                            unset($x_type);
                        }
                    }

                    $table->indexes = array();

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