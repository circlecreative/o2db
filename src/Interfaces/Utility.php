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

namespace O2System\O2DB\Interfaces;

// ------------------------------------------------------------------------

use O2System\O2Gears\Logger;

/**
 * Abstract Interfaces Utility Class
 *
 * Interface abstract class for Database Engine Driver, porting from CodeIgniter database utility class
 *
 * @package        O2System
 * @subpackage     core/gears
 * @category       core class
 * @author         Circle Creative Dev Team
 * @link           http://o2system.center/wiki/#GearsDebug
 *
 * @static         static class
 */
abstract class Utility
{
    /**
     * Database object
     *
     * @type    object
     */
    protected $_driver;

    // --------------------------------------------------------------------

    /**
     * List databases statement
     *
     * @type    string
     */
    protected $_list_databases = FALSE;

    /**
     * OPTIMIZE TABLE statement
     *
     * @type    string
     */
    protected $_optimize_table = FALSE;

    /**
     * REPAIR TABLE statement
     *
     * @type    string
     */
    protected $_repair_table = FALSE;

    // --------------------------------------------------------------------

    /**
     * Class constructor
     *
     * @param   object  &$driver Database Object
     */
    public function __construct( &$driver )
    {
        $this->_driver =& $driver;
        Logger::info( 'Database Utility Class Initialized' );
    }

    // --------------------------------------------------------------------

    /**
     * List databases
     *
     * @return  array
     * @throws  \Exception
     */
    public function list_databases()
    {
        // Is there a cached result?
        if( isset( $this->_driver->data_cache[ 'db_names' ] ) )
        {
            return $this->_driver->data_cache[ 'db_names' ];
        }
        elseif( $this->_list_databases === FALSE )
        {
            if( $this->_driver->debug_enabled )
            {
                throw new \Exception( 'Unsupported feature of the database platform you are using.' );
            }

            return FALSE;
        }

        $this->_driver->data_cache[ 'db_names' ] = array();

        $query = $this->_driver->query( $this->_list_databases );
        if( $query === FALSE )
        {
            return $this->_driver->data_cache[ 'db_names' ];
        }

        for( $i = 0, $query = $query->result_array(), $c = count( $query ); $i < $c; $i++ )
        {
            $this->_driver->data_cache[ 'db_names' ][ ] = current( $query[ $i ] );
        }

        return $this->_driver->data_cache[ 'db_names' ];
    }

    // --------------------------------------------------------------------

    /**
     * Determine if a particular database exists
     *
     * @param   string  $database_name
     *
     * @return  bool
     */
    public function database_exists( $database_name )
    {
        return in_array( $database_name, $this->list_databases() );
    }

    // --------------------------------------------------------------------

    /**
     * Optimize Table
     *
     * @param   string  $table_name
     *
     * @return  mixed
     * @throws  \Exception
     */
    public function optimize_table( $table_name )
    {
        if( $this->_optimize_table === FALSE )
        {
            if( $this->_driver->debug_enabled )
            {
                throw new \Exception( 'Unsupported feature of the database platform you are using.' );
            }

            return FALSE;
        }

        $query = $this->_driver->query( sprintf( $this->_optimize_table, $this->_driver->escape_identifiers( $table_name ) ) );
        if( $query !== FALSE )
        {
            $query = $query->result_array();

            return current( $query );
        }

        return FALSE;
    }

    // --------------------------------------------------------------------

    /**
     * Optimize Database
     *
     * @return  mixed
     * @throws  \Exception
     */
    public function optimize_database()
    {
        if( $this->_optimize_table === FALSE )
        {
            if( $this->_driver->debug_enabled )
            {
                throw new \Exception( 'Unsupported feature of the database platform you are using.' );
            }

            return FALSE;
        }

        $result = array();
        foreach( $this->_driver->list_tables() as $table_name )
        {
            $res = $this->_driver->query( sprintf( $this->_optimize_table, $this->_driver->escape_identifiers( $table_name ) ) );
            if( is_bool( $res ) )
            {
                return $res;
            }

            // Build the result array...
            $res = $res->result_array();
            $res = current( $res );
            $key = str_replace( $this->_driver->database . '.', '', current( $res ) );
            $keys = array_keys( $res );
            unset( $res[ $keys[ 0 ] ] );

            $result[ $key ] = $res;
        }

        return $result;
    }

    // --------------------------------------------------------------------

    /**
     * Repair Table
     *
     * @param   string  $table_name
     *
     * @return  mixed
     * @throws  \Exception
     */
    public function repair_table( $table_name )
    {
        if( $this->_repair_table === FALSE )
        {
            if( $this->_driver->debug_enabled )
            {
                throw new \Exception( 'Unsupported feature of the database platform you are using.' );
            }

            return FALSE;
        }

        $query = $this->_driver->query( sprintf( $this->_repair_table, $this->_driver->escape_identifiers( $table_name ) ) );
        if( is_bool( $query ) )
        {
            return $query;
        }

        $query = $query->result_array();

        return current( $query );
    }

    // --------------------------------------------------------------------

    /**
     * Generate CSV from a query result object
     *
     * @param   object  $query      Query result object
     * @param   string  $delim      Delimiter (default: ,)
     * @param   string  $newline    Newline character (default: \n)
     * @param   string  $enclosure  Enclosure (default: ")
     *
     * @return  string
     * @throws  \Exception
     */
    public function csv_from_result( $query, $delim = ',', $newline = "\n", $enclosure = '"' )
    {
        if( ! is_object( $query ) OR ! method_exists( $query, 'list_fields' ) )
        {
            throw new \Exception( 'You must submit a valid result object' );
        }

        $out = '';
        // First generate the headings from the table column names
        foreach( $query->list_fields() as $name )
        {
            $out .= $enclosure . str_replace( $enclosure, $enclosure . $enclosure, $name ) . $enclosure . $delim;
        }

        $out = substr( rtrim( $out ), 0, -strlen( $delim ) ) . $newline;

        // Next blast through the result array and build out the rows
        while( $row = $query->unbuffered_row( 'array' ) )
        {
            foreach( $row as $item )
            {
                $out .= $enclosure . str_replace( $enclosure, $enclosure . $enclosure, $item ) . $enclosure . $delim;
            }
            $out = substr( rtrim( $out ), 0, -strlen( $delim ) ) . $newline;
        }

        return $out;
    }

    // --------------------------------------------------------------------

    /**
     * Generate XML data from a query result object
     *
     * @param   object $query  Query result object
     * @param   array  $params Any preferences
     *
     * @return  string
     * @throws  \Exception
     */
    public function xml_from_result( $query, $params = array() )
    {
        if( ! is_object( $query ) OR ! method_exists( $query, 'list_fields' ) )
        {
            throw new \Exception( 'You must submit a valid result object' );
        }

        // Set our default values
        foreach( array( 'root' => 'root', 'element' => 'element', 'newline' => "\n", 'tab' => "\t" ) as $key => $val )
        {
            if( ! isset( $params[ $key ] ) )
            {
                $params[ $key ] = $val;
            }
        }

        // Create variables for convenience
        extract( $params );

        // Load the xml helper
        Autoloader::helper( 'xml' );

        // Generate the result
        $xml = '<' . $root . '>' . $newline;
        while( $row = $query->unbuffered_row() )
        {
            $xml .= $tab . '<' . $element . '>' . $newline;
            foreach( $row as $key => $val )
            {
                $xml .= $tab . $tab . '<' . $key . '>' . xml_convert( $val ) . '</' . $key . '>' . $newline;
            }
            $xml .= $tab . '</' . $element . '>' . $newline;
        }

        return $xml . '</' . $root . '>' . $newline;
    }

    // --------------------------------------------------------------------

    /**
     * Database Backup
     *
     * @param   array   $params
     *
     * @return  string
     */
    public function backup( $params = array() )
    {
        // If the parameters have not been submitted as an
        // array then we know that it is simply the table
        // name, which is a valid short cut.
        if( is_string( $params ) )
        {
            $params = array( 'tables' => $params );
        }

        // Set up our default preferences
        $prefs = array(
            'tables'             => array(),
            'ignore'             => array(),
            'filename'           => '',
            'format'             => 'gzip', // gzip, zip, txt
            'add_drop'           => TRUE,
            'add_insert'         => TRUE,
            'newline'            => "\n",
            'foreign_key_checks' => TRUE
        );

        // Did the user submit any preferences? If so set them....
        if( count( $params ) > 0 )
        {
            foreach( $prefs as $key => $val )
            {
                if( isset( $params[ $key ] ) )
                {
                    $prefs[ $key ] = $params[ $key ];
                }
            }
        }

        // Are we backing up a complete database or individual tables?
        // If no table names were submitted we'll fetch the entire table list
        if( count( $prefs[ 'tables' ] ) === 0 )
        {
            $prefs[ 'tables' ] = $this->_driver->list_tables();
        }

        // Validate the format
        if( ! in_array( $prefs[ 'format' ], array( 'gzip', 'zip', 'txt' ), TRUE ) )
        {
            $prefs[ 'format' ] = 'txt';
        }

        // Is the encoder supported? If not, we'll either issue an
        // error or use plain text depending on the debug settings
        if( ( $prefs[ 'format' ] === 'gzip' && ! function_exists( 'gzencode' ) )
            OR ( $prefs[ 'format' ] === 'zip' && ! function_exists( 'gzcompress' ) )
        )
        {
            if( $this->_driver->db_debug )
            {
                return $this->_driver->display_error( 'db_unsupported_compression' );
            }

            $prefs[ 'format' ] = 'txt';
        }

        // Was a Zip file requested?
        if( $prefs[ 'format' ] === 'zip' )
        {
            // Set the filename if not provided (only needed with Zip files)
            if( $prefs[ 'filename' ] === '' )
            {
                $prefs[ 'filename' ] = ( count( $prefs[ 'tables' ] ) === 1 ? $prefs[ 'tables' ] : $this->_driver->database )
                                       . date( 'Y-m-d_H-i', time() ) . '.sql';
            }
            else
            {
                // If they included the .zip file extension we'll remove it
                if( preg_match( '|.+?\.zip$|', $prefs[ 'filename' ] ) )
                {
                    $prefs[ 'filename' ] = str_replace( '.zip', '', $prefs[ 'filename' ] );
                }

                // Tack on the ".sql" file extension if needed
                if( ! preg_match( '|.+?\.sql$|', $prefs[ 'filename' ] ) )
                {
                    $prefs[ 'filename' ] .= '.sql';
                }
            }

            // Load the Zip class and output it
            $zip =& Autoloader::library( 'zip' );
            $zip->add_data( $prefs[ 'filename' ], $this->_backup( $prefs ) );

            return $zip->get_zip();
            //$CI =& \O2System::instance();
            //$CI->load->library('zip');
            //$CI->zip->add_data($prefs['filename'], $this->_backup($prefs));
            //return $CI->zip->get_zip();
        }
        elseif( $prefs[ 'format' ] === 'txt' ) // Was a text file requested?
        {
            return $this->_backup( $prefs );
        }
        elseif( $prefs[ 'format' ] === 'gzip' ) // Was a Gzip file requested?
        {
            return gzencode( $this->_backup( $prefs ) );
        }

        return;
    }

}
