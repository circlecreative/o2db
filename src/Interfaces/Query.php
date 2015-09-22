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

/**
 * Query Builder Class
 *
 * Porting from CodeIgniter Database Library
 *
 * This is the platform-independent base Query Builder implementation class.
 *
 * @package        O2System
 * @subpackage     Drivers
 * @category       Database
 * @author         Circle Creative Developer Team
 * @link           http://codeigniter.com/user_guide/database/
 *
 * @todo           Having with Math Operator
 */
abstract class Query
{
    private static $_operators      = array(
        'equal'         => '=',
        'not'           => '!=',
        'greater'       => '>',
        'less'          => '<',
        'greater_equal' => '>=',
        'less_equal'    => '<='
    );
    private static $_math_functions = array(
        'min', 'max', 'sum', 'avg', 'count'
    );


    /**
     * Return DELETE SQL flag
     *
     * @type    bool
     */
    protected $return_delete_sql = FALSE;

    /**
     * Reset DELETE data flag
     *
     * @type    bool
     */
    protected $reset_delete_data = FALSE;

    /**
     * QB SELECT data
     *
     * @type    array
     */
    protected $_select = array();

    /**
     * QB DISTINCT flag
     *
     * @type    bool
     */
    protected $_distinct = FALSE;

    /**
     * QB FROM data
     *
     * @type    array
     */
    protected $_from = array();

    /**
     * QB JOIN data
     *
     * @type    array
     */
    protected $_join = array();

    /**
     * QB WHERE data
     *
     * @type    array
     */
    protected $_where = array();

    /**
     * QB GROUP BY data
     *
     * @type    array
     */
    protected $_group_by = array();

    /**
     * QB HAVING data
     *
     * @type    array
     */
    protected $_having = array();

    /**
     * QB keys
     *
     * @type    array
     */
    protected $_keys = array();

    /**
     * QB LIMIT data
     *
     * @type    int
     */
    protected $_limit = FALSE;

    /**
     * QB OFFSET data
     *
     * @type    int
     */
    protected $_offset = FALSE;

    /**
     * QB ORDER BY data
     *
     * @type    array
     */
    protected $_order_by = array();

    /**
     * QB data sets
     *
     * @type    array
     */
    protected $_sets = array();

    /**
     * QB aliased tables list
     *
     * @type    array
     */
    protected $_aliased_tables = array();

    /**
     * QB No Escape data
     *
     * @type    array
     */
    protected $_no_escape = array();

    /**
     * QB WHERE group started flag
     *
     * @type    bool
     */
    protected $_where_group_started = FALSE;

    /**
     * QB WHERE group count
     *
     * @type    int
     */
    protected $_where_group_count = 0;

    // Query Builder Caching variables

    /**
     * QB Caching flag
     *
     * @type    bool
     */
    protected $_caching = FALSE;

    /**
     * QB Cache exists list
     *
     * @type    array
     */
    protected $_cache_exists = array();

    /**
     * QB Cache SELECT data
     *
     * @type    array
     */
    protected $_cache_select = array();

    /**
     * QB Cache FROM data
     *
     * @type    array
     */
    protected $_cache_from = array();

    /**
     * QB Cache JOIN data
     *
     * @type    array
     */
    protected $_cache_join = array();

    /**
     * QB Cache WHERE data
     *
     * @type    array
     */
    protected $_cache_where = array();

    /**
     * QB Cache GROUP BY data
     *
     * @type    array
     */
    protected $_cache_group_by = array();

    /**
     * QB Cache HAVING data
     *
     * @type    array
     */
    protected $_cache_having = array();

    /**
     * QB Cache ORDER BY data
     *
     * @type    array
     */
    protected $_cache_order_by = array();

    /**
     * QB Cache data sets
     *
     * @type    array
     */
    protected $_cache_sets = array();

    /**
     * QB Cache No Escape data
     *
     * @type    array
     */
    protected $_cache_no_escape = array();

    // --------------------------------------------------------------------

    /**
     * Select
     *
     * Generates the SELECT portion of the query
     *
     * @param   string  $select
     * @param   mixed   $escape
     *
     * @return  \O2System\O2DB\Interfaces\Query
     */
    public function select( $select = '*', $escape = NULL )
    {
        if( is_string( $select ) )
        {
            $select = explode( ',', $select );
        }

        // If the escape value was not set, we will base it on the global setting
        is_bool( $escape ) OR $escape = $this->_protect_identifiers;

        foreach( $select as $val )
        {
            $val = trim( $val );

            if( $val !== '' )
            {
                $this->_select[ ] = $val;
                $this->_no_escape[ ] = $escape;

                if( $this->_caching === TRUE )
                {
                    $this->_cache_select[ ] = $val;
                    $this->_cache_exists[ ] = 'select';
                    $this->_cache_no_escape[ ] = $escape;
                }
            }
        }

        return $this;
    }

    // --------------------------------------------------------------------

    /**
     * Select Max
     *
     * Generates a SELECT MAX(field) portion of a query
     *
     * @param   string  $select the field
     * @param   string  $alias  an alias
     *
     * @return  \O2System\O2DB\Interfaces\Query
     */
    public function select_max( $select = '', $alias = '' )
    {
        return $this->_max_min_avg_sum( $select, $alias, 'MAX' );
    }

    // --------------------------------------------------------------------

    /**
     * SELECT [MAX|MIN|AVG|SUM]()
     *
     * @used-by select_max()
     * @used-by select_min()
     * @used-by select_avg()
     * @used-by select_sum()
     *
     * @param   string  $select Field name
     * @param   string  $alias
     * @param   string  $type
     *
     * @return  \O2System\O2DB\Interfaces\Query
     * @throws  \Exception
     */
    protected function _max_min_avg_sum( $select = '', $alias = '', $type = 'MAX' )
    {
        if( ! is_string( $select ) OR $select === '' )
        {
            throw new \Exception( 'The query you submitted is not valid.' );
        }

        $type = strtoupper( $type );

        if( ! in_array( $type, array( 'MAX', 'MIN', 'AVG', 'SUM' ) ) )
        {
            throw new \Exception( 'Invalid function type: ' . $type );
        }

        if( $alias === '' )
        {
            $alias = $this->_create_alias_from_table( trim( $select ) );
        }

        $sql = $type . '(' . $this->protect_identifiers( trim( $select ) ) . ') AS ' . $this->escape_identifiers( trim( $alias ) );

        $this->_select[ ] = $sql;
        $this->_no_escape[ ] = NULL;

        if( $this->_caching === TRUE )
        {
            $this->_cache_select[ ] = $sql;
            $this->_cache_exists[ ] = 'select';
        }

        return $this;
    }

    // --------------------------------------------------------------------

    /**
     * Determines the alias name based on the table
     *
     * @param    string $item
     *
     * @return    string
     */
    protected function _create_alias_from_table( $item )
    {
        if( strpos( $item, '.' ) !== FALSE )
        {
            $item = explode( '.', $item );

            return end( $item );
        }

        return $item;
    }

    // --------------------------------------------------------------------

    /**
     * Select Min
     *
     * Generates a SELECT MIN(field) portion of a query
     *
     * @param    string    the field
     * @param    string    an alias
     *
     * @return    \O2System\O2DB\Interfaces\Query
     */
    public function select_min( $select = '', $alias = '' )
    {
        return $this->_max_min_avg_sum( $select, $alias, 'MIN' );
    }

    // --------------------------------------------------------------------

    /**
     * Select Average
     *
     * Generates a SELECT AVG(field) portion of a query
     *
     * @param    string    the field
     * @param    string    an alias
     *
     * @return    \O2System\O2DB\Interfaces\Query
     */
    public function select_avg( $select = '', $alias = '' )
    {
        return $this->_max_min_avg_sum( $select, $alias, 'AVG' );
    }

    // --------------------------------------------------------------------

    /**
     * Select Sum
     *
     * Generates a SELECT SUM(field) portion of a query
     *
     * @param    string    the field
     * @param    string    an alias
     *
     * @return    \O2System\O2DB\Interfaces\Query
     */
    public function select_sum( $select = '', $alias = '' )
    {
        return $this->_max_min_avg_sum( $select, $alias, 'SUM' );
    }

    // --------------------------------------------------------------------

    /**
     * DISTINCT
     *
     * Sets a flag which tells the query string compiler to add DISTINCT
     *
     * @param    bool $value
     *
     * @return    \O2System\O2DB\Interfaces\Query
     */
    public function distinct( $value = TRUE )
    {
        $this->_distinct = is_bool( $value ) ? $value : TRUE;

        return $this;
    }

    // --------------------------------------------------------------------

    /**
     * JOIN
     *
     * Generates the JOIN portion of the query
     *
     * @param    string
     * @param    string    the join condition
     * @param    string    the type of join
     * @param    string    whether not to try to escape identifiers
     *
     * @return    \O2System\O2DB\Interfaces\Query
     */
    public function join( $tables, $condition = '', $type = '', $escape = NULL )
    {
        if( is_array( $tables ) )
        {
            foreach( $tables as $table => $condition )
            {
                if( strpos( $table, ':' ) !== FALSE )
                {
                    $x_table = preg_split( '[:]', $table, -1, PREG_SPLIT_NO_EMPTY );
                    $table = end( $x_table );
                    $type = strtoupper( str_replace( '-', ' ', reset( $x_table ) ) );

                    $this->join( $table, $condition, $type );
                }
                else
                {
                    $this->join( $table, $condition );
                }
            }

            return $this;
        }
        else
        {
            if( $type !== '' )
            {
                $type = strtoupper( trim( $type ) );

                if( ! in_array( $type, array( 'LEFT', 'RIGHT', 'OUTER', 'INNER', 'LEFT OUTER', 'RIGHT OUTER' ),
                                TRUE )
                )
                {
                    $type = '';
                }
                else
                {
                    $type .= ' ';
                }
            }

            // Extract any aliases that might exist. We use this information
            // in the protect_identifiers to know whether to add a table prefix
            $this->_track_aliases( $tables );

            is_bool( $escape ) OR $escape = $this->_protect_identifiers;

            // Split multiple conditions
            if( $escape === TRUE && preg_match_all( '/\sAND\s|\sOR\s/i', $condition, $m, PREG_OFFSET_CAPTURE ) )
            {
                $new_condition = '';
                $m[ 0 ][ ] = array( '', strlen( $condition ) );

                for( $i = 0, $c = count( $m[ 0 ] ), $s = 0;
                     $i < $c;
                     $s = $m[ 0 ][ $i ][ 1 ] + strlen( $m[ 0 ][ $i ][ 0 ] ), $i++ )
                {
                    $temp = substr( $condition, $s, ( $m[ 0 ][ $i ][ 1 ] - $s ) );

                    $new_condition .= preg_match( "/([\[\]\w\.'-]+)(\s*[^\"\[`'\w]+\s*)(.+)/i", $temp, $match )
                        ? $this->protect_identifiers( $match[ 1 ] ) . $match[ 2 ] . $this->protect_identifiers( $match[ 3 ] )
                        : $temp;

                    $new_condition .= $m[ 0 ][ $i ][ 0 ];
                }

                $condition = ' ON ' . $new_condition;
            }
            // Split apart the condition and protect the identifiers
            elseif( $escape === TRUE && preg_match( "/([\[\]\w\.'-]+)(\s*[^\"\[`'\w]+\s*)(.+)/i", $condition,
                                                    $match )
            )
            {
                $condition = ' ON ' . $this->protect_identifiers( $match[ 1 ] ) . $match[ 2 ] . $this->protect_identifiers( $match[ 3 ] );
            }
            elseif( ! $this->_has_operator( $condition ) )
            {
                $condition = ' USING (' . ( $escape ? $this->escape_identifiers( $condition ) : $condition ) . ')';
            }
            else
            {
                $condition = ' ON ' . $condition;
            }

            // Do we want to escape the table name?
            if( $escape === TRUE )
            {
                $tables = $this->protect_identifiers( $tables, TRUE, NULL, FALSE );
            }

            // Assemble the JOIN statement
            $this->_join[ ] = $join = $type . 'JOIN ' . $tables . $condition;

            if( $this->_caching === TRUE )
            {
                $this->_cache_join[ ] = $join;
                $this->_cache_exists[ ] = 'join';
            }

            return $this;
        }
    }

    // --------------------------------------------------------------------

    /**
     * Track Aliases
     *
     * Used to track SQL statements written with aliased tables.
     *
     * @param   string $table  The table to inspect
     *
     * @return  string
     */
    protected function _track_aliases( $table )
    {
        if( is_array( $table ) )
        {
            foreach( $table as $t )
            {
                $this->_track_aliases( $t );
            }

            return;
        }

        // Does the string contain a comma?  If so, we need to separate
        // the string into discreet statements
        if( strpos( $table, ',' ) !== FALSE )
        {
            return $this->_track_aliases( explode( ',', $table ) );
        }

        // if a table alias is used we can recognize it by a space
        if( strpos( $table, ' ' ) !== FALSE )
        {
            // if the alias is written with the AS keyword, remove it
            $table = preg_replace( '/\s+AS\s+/i', ' ', $table );

            // Grab the alias
            $table = trim( strrchr( $table, ' ' ) );

            // Store the alias, if it doesn't already exist
            if( ! in_array( $table, $this->_aliased_tables ) )
            {
                $this->_aliased_tables[ ] = $table;
            }
        }
    }

    // --------------------------------------------------------------------

    /**
     * OR WHERE
     *
     * Generates the WHERE portion of the query.
     * Separates multiple calls with 'OR'.
     *
     * @param    mixed
     * @param    mixed
     * @param    bool
     *
     * @return    \O2System\O2DB\Interfaces\Query
     */
    public function or_where( $key, $value = NULL, $escape = NULL )
    {
        return $this->_wh( '_where', $key, $value, 'OR ', $escape );
    }

    // --------------------------------------------------------------------

    /**
     * WHERE, HAVING
     *
     * @used-by    where()
     * @used-by    or_where()
     * @used-by    having()
     * @used-by    or_having()
     *
     * @param    string $property '_where' or '_having'
     * @param    mixed  $key
     * @param    mixed  $value
     * @param    string $type
     * @param    bool   $escape
     *
     * @return    \O2System\O2DB\Interfaces\Query
     */
    protected function _wh( $property, $key, $value = NULL, $type = 'AND ', $escape = NULL )
    {
        $_cache_key = ( $property === '_having' ) ? '_cache_having' : '_cache_where';

        if( ! is_array( $key ) )
        {
            $key = array( $key => $value );
        }

        // If the escape value was not set will base it on the global setting
        is_bool( $escape ) OR $escape = $this->_protect_identifiers;

        foreach( $key as $k => $v )
        {
            if( strpos( $k, ':' ) !== FALSE )
            {
                $x_k = preg_split( '[:]', $k, -1, PREG_SPLIT_NO_EMPTY );
                $k = reset( $x_k );
                $op = end( $x_k );

                $op = isset( static::$_operators[ $op ] ) ? static::$_operators[ $op ] : '=';
                $k = $k . ' ' . $op;
            }

            $prefix = ( count( $this->$property ) === 0 && count( $this->$_cache_key ) === 0 )
                ? $this->_group_get_type( '' )
                : $this->_group_get_type( $type );

            if( $v !== NULL )
            {
                if( $escape === TRUE )
                {
                    $v = ' ' . $this->escape( $v );
                }

                if( ! $this->_has_operator( $k ) )
                {
                    $k .= ' = ';
                }
            }
            elseif( ! $this->_has_operator( $k ) )
            {
                // value appears not to have been set, assign the test to IS NULL
                $k .= ' IS NULL';
            }
            elseif( preg_match( '/\s*(!?=|<>|IS(?:\s+NOT)?)\s*$/i', $k, $match, PREG_OFFSET_CAPTURE ) )
            {
                $k = substr( $k, 0, $match[ 0 ][ 1 ] ) . ( $match[ 1 ][ 0 ] === '=' ? ' IS NULL' : ' IS NOT NULL' );
            }

            $this->{$property}[ ] = array( 'condition' => $prefix . $k . $v, 'escape' => $escape );
            if( $this->_caching === TRUE )
            {
                $this->{$_cache_key}[ ] = array( 'condition' => $prefix . $k . $v, 'escape' => $escape );
                $this->_cache_exists[ ] = substr( $property, 3 );
            }

        }

        return $this;
    }

    // --------------------------------------------------------------------

    /**
     * Group_get_type
     *
     * @used-by    group_start()
     * @used-by    _like()
     * @used-by    _wh()
     * @used-by    _where_in()
     *
     * @param    string $type
     *
     * @return    string
     */
    protected function _group_get_type( $type )
    {
        if( $this->_where_group_started )
        {
            $type = '';
            $this->_where_group_started = FALSE;
        }

        return $type;
    }

    // --------------------------------------------------------------------

    /**
     * WHERE IN
     *
     * Generates a WHERE field IN('item', 'item') SQL query,
     * joined with 'AND' if appropriate.
     *
     * @param    string $key    The field to search
     * @param    array  $values The values searched on
     * @param    bool   $escape
     *
     * @return    \O2System\O2DB\Interfaces\Query
     */
    public function where_in( $key = NULL, $values = NULL, $escape = NULL )
    {
        return $this->_where_in( $key, $values, FALSE, 'AND ', $escape );
    }

    // --------------------------------------------------------------------

    /**
     * Internal WHERE IN
     *
     * @used-by    where_in()
     * @used-by    or_where_in()
     * @used-by    where_not_in()
     * @used-by    or_where_not_in()
     *
     * @param    string $field    The field to search
     * @param    array  $values The values searched on
     * @param    bool   $not    If the statement would be IN or NOT IN
     * @param    string $type
     * @param    bool   $escape
     *
     * @return    \O2System\O2DB\Interfaces\Query
     */
    protected function _where_in( $field = NULL, $values = NULL, $not = FALSE, $type = 'AND ', $escape = NULL )
    {
        if( $field === NULL OR $values === NULL )
        {
            return $this;
        }

        if( ! is_array( $values ) )
        {
            $values = array( $values );
        }

        is_bool( $escape ) OR $escape = $this->_protect_identifiers;

        $not = ( $not ) ? ' NOT' : '';

        $where_in = array();
        foreach( $values as $value )
        {
            $where_in[ ] = $this->escape( $value );
        }

        $prefix = ( count( $this->_where ) === 0 ) ? $this->_group_get_type( '' ) : $this->_group_get_type( $type );
        $where_in = array(
            'condition' => $prefix . $field . $not . ' IN(' . implode( ', ', $where_in ) . ')',
            'escape'    => $escape
        );

        $this->_where[ ] = $where_in;
        if( $this->_caching === TRUE )
        {
            $this->_cache_where[ ] = $where_in;
            $this->_cache_exists[ ] = 'where';
        }

        return $this;
    }

    // --------------------------------------------------------------------

    /**
     * OR WHERE IN
     *
     * Generates a WHERE field IN('item', 'item') SQL query,
     * joined with 'OR' if appropriate.
     *
     * @param    string $field    The field to search
     * @param    array  $values The values searched on
     * @param    bool   $escape
     *
     * @return    \O2System\O2DB\Interfaces\Query
     */
    public function or_where_in( $field = NULL, $values = NULL, $escape = NULL )
    {
        return $this->_where_in( $field, $values, FALSE, 'OR ', $escape );
    }

    // --------------------------------------------------------------------

    /**
     * WHERE NOT IN
     *
     * Generates a WHERE field NOT IN('item', 'item') SQL query,
     * joined with 'AND' if appropriate.
     *
     * @param    string $key    The field to search
     * @param    array  $values The values searched on
     * @param    bool   $escape
     *
     * @return    \O2System\O2DB\Interfaces\Query
     */
    public function where_not_in( $key = NULL, $values = NULL, $escape = NULL )
    {
        return $this->_where_in( $key, $values, TRUE, 'AND ', $escape );
    }

    // --------------------------------------------------------------------

    /**
     * OR WHERE NOT IN
     *
     * Generates a WHERE field NOT IN('item', 'item') SQL query,
     * joined with 'OR' if appropriate.
     *
     * @param    string $key    The field to search
     * @param    array  $values The values searched on
     * @param    bool   $escape
     *
     * @return    \O2System\O2DB\Interfaces\Query
     */
    public function or_where_not_in( $key = NULL, $values = NULL, $escape = NULL )
    {
        return $this->_where_in( $key, $values, TRUE, 'OR ', $escape );
    }

    // --------------------------------------------------------------------

    /**
     * LIKE
     *
     * Generates a %LIKE% portion of the query.
     * Separates multiple calls with 'AND'.
     *
     * @param    mixed  $field
     * @param    string $match
     * @param    string $side
     * @param    bool   $escape
     *
     * @return    \O2System\O2DB\Interfaces\Query
     */
    public function like( $field, $match = '', $side = 'both', $escape = NULL )
    {
        return $this->_like( $field, $match, 'AND ', $side, '', $escape );
    }

    // --------------------------------------------------------------------

    /**
     * Internal LIKE
     *
     * @used-by    like()
     * @used-by    or_like()
     * @used-by    not_like()
     * @used-by    or_not_like()
     *
     * @param    mixed  $field
     * @param    string $match
     * @param    string $type
     * @param    string $side
     * @param    string $not
     * @param    bool   $escape
     *
     * @return    \O2System\O2DB\Interfaces\Query
     */
    protected function _like( $field, $match = '', $type = 'AND ', $side = 'both', $not = '', $escape = NULL )
    {
        if( ! is_array( $field ) )
        {
            $field = array( $field => $match );
        }

        is_bool( $escape ) OR $escape = $this->_protect_identifiers;

        foreach( $field as $key => $value )
        {
            $prefix = ( count( $this->_where ) === 0 && count( $this->_cache_where ) === 0 )
                ? $this->_group_get_type( '' ) : $this->_group_get_type( $type );

            $value = $this->escape_like_string( $value );

            if( $side === 'none' )
            {
                $like_statement = "{$prefix} {$key} {$not} LIKE '{$value}'";
            }
            elseif( $side === 'before' )
            {
                $like_statement = "{$prefix} {$key} {$not} LIKE '%{$value}'";
            }
            elseif( $side === 'after' )
            {
                $like_statement = "{$prefix} {$key} {$not} LIKE '{$value}%'";
            }
            else
            {
                $like_statement = "{$prefix} {$key} {$not} LIKE '%{$value}%'";
            }

            // some platforms require an escape sequence definition for LIKE wildcards
            if( $this->_like_escape_string !== '' )
            {
                $like_statement .= sprintf( $this->_like_escape_string, $this->_like_escape_character );
            }

            $this->_where[ ] = array( 'condition' => $like_statement, 'escape' => $escape );
            if( $this->_caching === TRUE )
            {
                $this->_cache_where[ ] = array( 'condition' => $like_statement, 'escape' => $escape );
                $this->_cache_exists[ ] = 'where';
            }
        }

        return $this;
    }

    // --------------------------------------------------------------------

    /**
     * NOT LIKE
     *
     * Generates a NOT LIKE portion of the query.
     * Separates multiple calls with 'AND'.
     *
     * @param    mixed  $field
     * @param    string $match
     * @param    string $side
     * @param    bool   $escape
     *
     * @return    \O2System\O2DB\Interfaces\Query
     */
    public function not_like( $field, $match = '', $side = 'both', $escape = NULL )
    {
        return $this->_like( $field, $match, 'AND ', $side, 'NOT', $escape );
    }

    // --------------------------------------------------------------------

    /**
     * OR LIKE
     *
     * Generates a %LIKE% portion of the query.
     * Separates multiple calls with 'OR'.
     *
     * @param    mixed  $field
     * @param    string $match
     * @param    string $side
     * @param    bool   $escape
     *
     * @return    \O2System\O2DB\Interfaces\Query
     */
    public function or_like( $field, $match = '', $side = 'both', $escape = NULL )
    {
        return $this->_like( $field, $match, 'OR ', $side, '', $escape );
    }

    // --------------------------------------------------------------------

    /**
     * OR NOT LIKE
     *
     * Generates a NOT LIKE portion of the query.
     * Separates multiple calls with 'OR'.
     *
     * @param    mixed  $field
     * @param    string $match
     * @param    string $side
     * @param    bool   $escape
     *
     * @return    \O2System\O2DB\Interfaces\Query
     */
    public function or_not_like( $field, $match = '', $side = 'both', $escape = NULL )
    {
        return $this->_like( $field, $match, 'OR ', $side, 'NOT', $escape );
    }

    // --------------------------------------------------------------------

    /**
     * Starts a query group, but ORs the group
     *
     * @return    \O2System\O2DB\Interfaces\Query
     */
    public function or_group_start()
    {
        return $this->group_start( '', 'OR ' );
    }

    // --------------------------------------------------------------------

    /**
     * Starts a query group.
     *
     * @param    string $not  (Internal use only)
     * @param    string $type (Internal use only)
     *
     * @return    \O2System\O2DB\Interfaces\Query
     */
    public function group_start( $not = '', $type = 'AND ' )
    {
        $type = $this->_group_get_type( $type );

        $this->_where_group_started = TRUE;
        $prefix = ( count( $this->_where ) === 0 && count( $this->_cache_where ) === 0 ) ? '' : $type;
        $where = array(
            'condition' => $prefix . $not . str_repeat( ' ', ++$this->_where_group_count ) . ' (',
            'escape'    => FALSE
        );

        $this->_where[ ] = $where;
        if( $this->_caching )
        {
            $this->_cache_where[ ] = $where;
        }

        return $this;
    }

    // --------------------------------------------------------------------

    /**
     * Starts a query group, but NOTs the group
     *
     * @return    \O2System\O2DB\Interfaces\Query
     */
    public function not_group_start()
    {
        return $this->group_start( 'NOT ', 'AND ' );
    }

    // --------------------------------------------------------------------

    /**
     * Starts a query group, but OR NOTs the group
     *
     * @return    \O2System\O2DB\Interfaces\Query
     */
    public function or_not_group_start()
    {
        return $this->group_start( 'NOT ', 'OR ' );
    }

    // --------------------------------------------------------------------

    /**
     * Ends a query group
     *
     * @return    \O2System\O2DB\Interfaces\Query
     */
    public function group_end()
    {
        $this->_where_group_started = FALSE;
        $where = array(
            'condition' => str_repeat( ' ', $this->_where_group_count-- ) . ')',
            'escape'    => FALSE
        );

        $this->_where[ ] = $where;
        if( $this->_caching )
        {
            $this->_cache_where[ ] = $where;
        }

        return $this;
    }

    // --------------------------------------------------------------------

    /**
     * GROUP BY
     *
     * @param    string|array   $fields
     * @param    bool           $escape
     *
     * @return    \O2System\O2DB\Interfaces\Query
     */
    public function group_by( $fields, $escape = NULL )
    {
        is_bool( $escape ) OR $escape = $this->_protect_identifiers;

        if( is_string( $fields ) )
        {
            $fields = ( $escape === TRUE )
                ? explode( ',', $fields )
                : array( $fields );
        }

        foreach( $fields as $field )
        {
            $field = trim( $field );

            if( $field !== '' )
            {
                $field = array( 'field' => $field, 'escape' => $escape );

                $this->_group_by[ ] = $field;
                if( $this->_caching === TRUE )
                {
                    $this->_cache_group_by[ ] = $field;
                    $this->_cache_exists[ ] = 'group_by';
                }
            }
        }

        return $this;
    }

    // --------------------------------------------------------------------

    /**
     * HAVING
     *
     * Separates multiple calls with 'AND'.
     *
     * @param    string $key
     * @param    string $value
     * @param    bool   $escape
     *
     * @return    object
     */
    public function having( $key, $value = NULL, $escape = NULL )
    {
        return $this->_wh( '_having', $key, $value, 'AND ', $escape );
    }

    // --------------------------------------------------------------------

    /**
     * OR HAVING
     *
     * Separates multiple calls with 'OR'.
     *
     * @param    string $key
     * @param    string $value
     * @param    bool   $escape
     *
     * @return    object
     */
    public function or_having( $key, $value = NULL, $escape = NULL )
    {
        return $this->_wh( '_having', $key, $value, 'OR ', $escape );
    }

    // --------------------------------------------------------------------

    /**
     * ORDER BY
     *
     * @param    string $order_by
     * @param    string $direction ASC, DESC or RANDOM
     * @param    bool   $escape
     *
     * @return    \O2System\O2DB\Interfaces\Query
     */
    public function order_by( $order_by, $direction = '', $escape = NULL )
    {
        $direction = strtoupper( trim( $direction ) );

        if( $direction === 'RANDOM' )
        {
            $direction = '';

            // Do we have a seed value?
            $order_by = ctype_digit( (string)$order_by )
                ? sprintf( $this->_random_keywords[ 1 ], $order_by )
                : $this->_random_keywords[ 0 ];
        }
        elseif( empty( $order_by ) )
        {
            return $this;
        }
        elseif( $direction !== '' )
        {
            $direction = in_array( $direction, array( 'ASC', 'DESC' ), TRUE ) ? ' ' . $direction : '';
        }

        is_bool( $escape ) OR $escape = $this->_protect_identifiers;

        if( $escape === FALSE )
        {
            $order_by[ ] = array( 'field' => $order_by, 'direction' => $direction, 'escape' => FALSE );
        }
        else
        {
            $order_by = array();
            foreach( explode( ',', $order_by ) as $field )
            {
                $order_by[ ] = ( $direction === '' && preg_match( '/\s+(ASC|DESC)$/i', rtrim( $field ), $match, PREG_OFFSET_CAPTURE ) )
                    ? array(
                        'field'     => ltrim( substr( $field, 0, $match[ 0 ][ 1 ] ) ),
                        'direction' => ' ' . $match[ 1 ][ 0 ], 'escape' => TRUE
                    )
                    : array( 'field' => trim( $field ), 'direction' => $direction, 'escape' => TRUE );
            }
        }

        $this->_order_by = array_merge( $this->_order_by, $order_by );
        if( $this->_caching === TRUE )
        {
            $this->_cache_order_by = array_merge( $this->_cache_order_by, $order_by );
            $this->_cache_exists[ ] = 'order_by';
        }

        return $this;
    }

    // --------------------------------------------------------------------

    /**
     * Sets the OFFSET value
     *
     * @param    int $offset OFFSET value
     *
     * @return    \O2System\O2DB\Interfaces\Query
     */
    public function offset( $offset )
    {
        empty( $offset ) OR $this->_offset = (int)$offset;

        return $this;
    }

    // --------------------------------------------------------------------

    /**
     * Get SELECT query string
     *
     * Compiles a SELECT query string and returns the sql.
     *
     * @param    string    the table name to select from (optional)
     * @param    bool      TRUE: resets QB values; FALSE: leave QB vaules alone
     *
     * @return    string
     */
    public function get_compiled_select( $table = '', $reset = TRUE )
    {
        if( $table !== '' )
        {
            $this->_track_aliases( $table );
            $this->from( $table );
        }

        $select = $this->_compile_select();

        if( $reset === TRUE )
        {
            $this->_reset_select();
        }

        return $select;
    }

    // --------------------------------------------------------------------

    /**
     * From
     *
     * Generates the FROM portion of the query
     *
     * @param    mixed $tables can be a string or array
     *
     * @return    \O2System\O2DB\Interfaces\Query
     */
    public function from( $tables )
    {
        foreach( (array)$tables as $table )
        {
            if( strpos( $table, ',' ) !== FALSE )
            {
                foreach( explode( ',', $table ) as $x_table )
                {
                    $x_table = trim( $x_table );
                    $this->_track_aliases( $x_table );

                    $this->_from[ ] = $x_table = $this->protect_identifiers( $x_table, TRUE, NULL, FALSE );

                    if( $this->_caching === TRUE )
                    {
                        $this->_cache_from[ ] = $x_table;
                        $this->_cache_exists[ ] = 'from';
                    }
                }
            }
            else
            {
                $table = trim( $table );

                // Extract any aliases that might exist. We use this information
                // in the protect_identifiers to know whether to add a table prefix
                $this->_track_aliases( $table );

                $this->_from[ ] = $table = $this->protect_identifiers( $table, TRUE, NULL, FALSE );

                if( $this->_caching === TRUE )
                {
                    $this->_cache_from[ ] = $table;
                    $this->_cache_exists[ ] = 'from';
                }
            }
        }

        return $this;
    }

    // --------------------------------------------------------------------

    /**
     * Compile the SELECT statement
     *
     * Generates a query string based on which functions were used.
     * Should not be called directly.
     *
     * @param   bool    $select_override
     *
     * @return  string
     */
    protected function _compile_select( $select_override = FALSE )
    {
        // Combine any cached components with the current statements
        $this->_merge_cache();

        // Write the "select" portion of the query
        if( $select_override !== FALSE )
        {
            $sql = $select_override;
        }
        else
        {
            $sql = ( ! $this->_distinct ) ? 'SELECT ' : 'SELECT DISTINCT ';

            if( count( $this->_select ) === 0 )
            {
                $sql .= '*';
            }
            else
            {
                // Cycle through the "select" portion of the query and prep each column name.
                // The reason we protect identifiers here rather then in the select() function
                // is because until the user calls the from() function we don't know if there are aliases
                foreach( $this->_select as $key => $val )
                {
                    $no_escape = isset( $this->_no_escape[ $key ] ) ? $this->_no_escape[ $key ] : NULL;
                    $this->_select[ $key ] = $this->protect_identifiers( $val, FALSE, $no_escape );
                }

                $sql .= implode( ', ', $this->_select );
            }
        }

        // Write the "FROM" portion of the query
        if( count( $this->_from ) > 0 )
        {
            $sql .= "\nFROM " . $this->_from_tables();
        }

        // Write the "JOIN" portion of the query
        if( count( $this->_join ) > 0 )
        {
            $sql .= "\n" . implode( "\n", $this->_join );
        }

        $sql .= $this->_compile_where( '_where' ) . $this->_compile_group_by() . $this->_compile_where( '_having' ) . $this->_compile_order_by(); // ORDER BY

        // LIMIT
        if( $this->_limit )
        {
            return $this->_limit( $sql . "\n" );
        }

        return $sql;
    }

    // --------------------------------------------------------------------

    /**
     * Merge Cache
     *
     * When called, this function merges any cached QB arrays with
     * locally called ones.
     *
     * @return    void
     */
    protected function _merge_cache()
    {
        if( count( $this->_cache_exists ) === 0 )
        {
            return;
        }
        elseif( in_array( 'select', $this->_cache_exists, TRUE ) )
        {
            $no_escape = $this->_cache_no_escape;
        }

        foreach( array_unique( $this->_cache_exists ) as $cache ) // select, from, etc.
        {
            $variable = '_' . $cache;
            $cache_variable = '_cache_' . $cache;
            $new_variable = $this->$cache_variable;

            for( $i = 0, $c = count( $this->$variable ); $i < $c; $i++ )
            {
                if( ! in_array( $this->{$variable}[ $i ], $new_variable, TRUE ) )
                {
                    $new_variable[ ] = $this->{$variable}[ $i ];
                    if( $cache === 'select' )
                    {
                        $no_escape[ ] = $this->_no_escape[ $i ];
                    }
                }
            }

            $this->$variable = $new_variable;
            if( $cache === 'select' )
            {
                $this->_no_escape = $no_escape;
            }
        }

        // If we are "protecting identifiers" we need to examine the "from"
        // portion of the query to determine if there are any aliases
        if( $this->_protect_identifiers === TRUE && count( $this->_cache_from ) > 0 )
        {
            $this->_track_aliases( $this->_from );
        }
    }

    // --------------------------------------------------------------------

    /**
     * FROM tables
     *
     * Groups tables in FROM clauses if needed, so there is no confusion
     * about operator precedence.
     *
     * Note: This is only used (and overridden) by MySQL and CUBRID.
     *
     * @return  string
     */
    protected function _from_tables()
    {
        return implode( ', ', $this->_from );
    }

    // --------------------------------------------------------------------

    /**
     * Compile WHERE, HAVING statements
     *
     * Escapes identifiers in WHERE and HAVING statements at execution time.
     *
     * Required so that aliases are tracked properly, regardless of wether
     * where(), or_where(), having(), or_having are called prior to from(),
     * join() and prefix_table is added only if needed.
     *
     * @param    string $property '_where' or '_having'
     *
     * @return  string  SQL statement
     */
    protected function _compile_where( $property )
    {
        if( count( $this->$property ) > 0 )
        {
            for( $i = 0, $c = count( $this->$property ); $i < $c; $i++ )
            {
                // Is this condition already compiled?
                if( is_string( $this->{$property}[ $i ] ) )
                {
                    continue;
                }
                elseif( $this->{$property}[ $i ][ 'escape' ] === FALSE )
                {
                    $this->{$property}[ $i ] = $this->{$property}[ $i ][ 'condition' ];
                    continue;
                }

                // Split multiple conditions
                $conditions = preg_split(
                    '/(\s*AND\s+|\s*OR\s+)/i',
                    $this->{$property}[ $i ][ 'condition' ],
                    -1,
                    PREG_SPLIT_DELIM_CAPTURE | PREG_SPLIT_NO_EMPTY
                );

                for( $ci = 0, $cc = count( $conditions ); $ci < $cc; $ci++ )
                {
                    if( ( $op = $this->_get_operator( $conditions[ $ci ] ) ) === FALSE OR
                        ! preg_match( '/^(\(?)(.*)(' . preg_quote( $op, '/' ) . ')\s*(.*(?<!\)))?(\)?)$/i', $conditions[ $ci ], $matches )
                    )
                    {
                        continue;
                    }

                    // $matches = array(
                    //	0 => '(test <= foo)',	/* the whole thing */
                    //	1 => '(',		/* optional */
                    //	2 => 'test',		/* the field name */
                    //	3 => ' <= ',		/* $op */
                    //	4 => 'foo',		/* optional, if $op is e.g. 'IS NULL' */
                    //	5 => ')'		/* optional */
                    // );

                    if( ! empty( $matches[ 4 ] ) )
                    {
                        $this->_is_literal( $matches[ 4 ] ) OR $matches[ 4 ] = $this->protect_identifiers( trim( $matches[ 4 ] ) );
                        $matches[ 4 ] = ' ' . $matches[ 4 ];
                    }

                    $conditions[ $ci ] = $matches[ 1 ] . $this->protect_identifiers( trim( $matches[ 2 ] ) )
                                         . ' ' . trim( $matches[ 3 ] ) . $matches[ 4 ] . $matches[ 5 ];
                }

                $this->{$property}[ $i ] = implode( '', $conditions );
            }

            return ( $property === '_having' ? "\nHAVING " : "\nWHERE " ) . implode( "\n", $this->$property );
        }

        return '';
    }

    // --------------------------------------------------------------------

    /**
     * Is literal
     *
     * Determines if a string represents a literal value or a field name
     *
     * @param   string  $string
     *
     * @return  bool
     */
    protected function _is_literal( $string )
    {
        $string = trim( $string );

        if( empty( $string ) OR
            ctype_digit( $string ) OR
            (string)(float)$string === $string OR
            in_array( strtoupper( $string ), array( 'TRUE', 'FALSE' ), TRUE )
        )
        {
            return TRUE;
        }

        static $_string;

        if( empty( $_string ) )
        {
            $_string = ( $this->_escape_character !== '"' )
                ? array( '"', "'" ) : array( "'" );
        }

        return in_array( $string[ 0 ], $_string, TRUE );
    }

    // --------------------------------------------------------------------

    /**
     * Compile GROUP BY
     *
     * Escapes identifiers in GROUP BY statements at execution time.
     *
     * Required so that aliases are tracked properly, regardless of wether
     * group_by() is called prior to from(), join() and prefix_table is added
     * only if needed.
     *
     * @return  string  SQL statement
     */
    protected function _compile_group_by()
    {
        if( count( $this->_group_by ) > 0 )
        {
            for( $i = 0, $c = count( $this->_group_by ); $i < $c; $i++ )
            {
                // Is it already compiled?
                if( is_string( $this->_group_by[ $i ] ) )
                {
                    continue;
                }

                $this->_group_by[ $i ] = ( $this->_group_by[ $i ][ 'escape' ] === FALSE OR $this->_is_literal( $this->_group_by[ $i ][ 'field' ] ) )
                    ? $this->_group_by[ $i ][ 'field' ]
                    : $this->protect_identifiers( $this->_group_by[ $i ][ 'field' ] );
            }

            return "\nGROUP BY " . implode( ', ', $this->_group_by );
        }

        return '';
    }

    // --------------------------------------------------------------------

    /**
     * Compile ORDER BY
     *
     * Escapes identifiers in ORDER BY statements at execution time.
     *
     * Required so that aliases are tracked properly, regardless of wether
     * order_by() is called prior to from(), join() and prefix_table is added
     * only if needed.
     *
     * @return  string    SQL statement
     */
    protected function _compile_order_by()
    {
        if( is_array( $this->_order_by ) && count( $this->_order_by ) > 0 )
        {
            for( $i = 0, $c = count( $this->_order_by ); $i < $c; $i++ )
            {
                if( $this->_order_by[ $i ][ 'escape' ] !== FALSE && ! $this->_is_literal( $this->_order_by[ $i ][ 'field' ] ) )
                {
                    $this->_order_by[ $i ][ 'field' ] = $this->protect_identifiers( $this->_order_by[ $i ][ 'field' ] );
                }

                $this->_order_by[ $i ] = $this->_order_by[ $i ][ 'field' ] . $this->_order_by[ $i ][ 'direction' ];
            }

            return $this->_order_by = "\nORDER BY " . implode( ', ', $this->_order_by );
        }
        elseif( is_string( $this->_order_by ) )
        {
            return $this->_order_by;
        }

        return '';
    }

    // --------------------------------------------------------------------

    /**
     * LIMIT string
     *
     * Generates a platform-specific LIMIT clause.
     *
     * @param   string $sql SQL Query
     *
     * @return  string
     */
    protected function _limit( $sql )
    {
        return $sql . ' LIMIT ' . ( $this->_offset ? $this->_offset . ', ' : '' ) . $this->_limit;
    }

    // --------------------------------------------------------------------

    /**
     * Resets the query builder values.  Called by the get() function
     *
     * @return    void
     */
    protected function _reset_select()
    {
        $this->_reset_run( array(
                               '_select'         => array(),
                               '_from'           => array(),
                               '_join'           => array(),
                               '_where'          => array(),
                               '_group_by'        => array(),
                               '_having'         => array(),
                               '_order_by'        => array(),
                               '_aliased_tables' => array(),
                               '_no_escape'      => array(),
                               '_distinct'       => FALSE,
                               '_limit'          => FALSE,
                               '_offset'         => FALSE
                           ) );
    }

    // --------------------------------------------------------------------

    /**
     * Resets the query builder values.  Called by the get() function
     *
     * @param   array $reset_items An array of fields to reset
     *
     * @return  void
     */
    protected function _reset_run( $reset_items )
    {
        foreach( $reset_items as $item => $default_value )
        {
            $this->$item = $default_value;
        }
    }

    // --------------------------------------------------------------------

    /**
     * Get
     *
     * Compiles the select statement based on the other functions called
     * and runs the query
     *
     * @param    string    the table
     * @param    string    the limit clause
     * @param    string    the offset clause
     *
     * @return    object
     */
    public function get( $table = '', $limit = NULL, $offset = NULL )
    {
        if( $table !== '' )
        {
            $this->_track_aliases( $table );
            $this->from( $table );
        }

        if( ! empty( $limit ) )
        {
            $this->limit( $limit, $offset );
        }

        $result = $this->query( $this->_compile_select() );
        $this->_reset_select();

        return $result;
    }

    // --------------------------------------------------------------------

    /**
     * LIMIT
     *
     * @param    int $value  LIMIT value
     * @param    int $offset OFFSET value
     *
     * @return    \O2System\O2DB\Interfaces\Query
     */
    public function limit( $value, $offset = 0 )
    {
        is_null( $value ) OR $this->_limit = (int)$value;
        empty( $offset ) OR $this->_offset = (int)$offset;

        return $this;
    }

    // --------------------------------------------------------------------

    /**
     * "Count All Results" query
     *
     * Generates a platform-specific query string that counts all records
     * returned by an Query Builder query.
     *
     * @param   string $table
     *
     * @return  int
     */
    public function count_all_results( $table = '' )
    {
        if( $table !== '' )
        {
            $this->_track_aliases( $table );
            $this->from( $table );
        }

        $result = ( $this->_distinct === TRUE )
            ? $this->query( $this->_count_string . $this->protect_identifiers( 'numrows' ) . "\nFROM (\n" . $this->_compile_select() . "\n) CI_count_all_results" )
            : $this->query( $this->_compile_select( $this->_count_string . $this->protect_identifiers( 'numrows' ) ) );

        $this->_reset_select();

        if( $result->num_rows() === 0 )
        {
            return 0;
        }

        $row = $result->row();

        return (int)$row->numrows;
    }

    // --------------------------------------------------------------------

    /**
     * Get_Where
     *
     * Allows the where clause, limit and offset to be added directly
     *
     * @param   string $table
     * @param   string $where
     * @param   int    $limit
     * @param   int    $offset
     *
     * @return  object
     */
    public function get_where( $table = '', $where = NULL, $limit = NULL, $offset = NULL )
    {
        if( $table !== '' )
        {
            $this->from( $table );
        }

        if( $where !== NULL )
        {
            $this->where( $where );
        }

        if( ! empty( $limit ) )
        {
            $this->limit( $limit, $offset );
        }

        $result = $this->query( $this->_compile_select() );
        $this->_reset_select();

        return $result;
    }

    // --------------------------------------------------------------------

    /**
     * WHERE
     *
     * Generates the WHERE portion of the query.
     * Separates multiple calls with 'AND'.
     *
     * @param   mixed $key
     * @param   mixed $value
     * @param   bool  $escape
     *
     * @return  \O2System\O2DB\Interfaces\Query
     */
    public function where( $key, $value = NULL, $escape = NULL )
    {
        return $this->_wh( '_where', $key, $value, 'AND ', $escape );
    }

    // --------------------------------------------------------------------

    /**
     * Insert_Batch
     *
     * Compiles batch insert strings and runs the queries
     *
     * @param   string $table  Table to insert into
     * @param   array  $set    An associative array of insert values
     * @param   bool   $escape Whether to escape values and identifiers
     *
     * @return  int Number of rows inserted or FALSE on failure
     * @throws  \Exception
     */
    public function insert_batch( $table = '', $set = NULL, $escape = NULL )
    {
        if( $set !== NULL )
        {
            $this->set_insert_batch( $set, '', $escape );
        }

        if( count( $this->_sets ) === 0 )
        {
            // No valid data array. Folds in cases where keys and values did not match up
            if( $this->debug_enabled )
            {
                throw new \Exception( 'You must use the "set" method to update an entry.' );
            }

            return FALSE;
        }

        if( $table === '' )
        {
            if( ! isset( $this->_from[ 0 ] ) )
            {
                if( $this->debug_enabled )
                {
                    throw new \Exception( 'You must set the database table to be used with your query.' );
                }

                return FALSE;
            }

            $table = $this->_from[ 0 ];
        }

        // Batch this baby
        $affected_rows = 0;
        for( $i = 0, $total = count( $this->_sets ); $i < $total; $i += 100 )
        {
            $this->query( $this->_insert_batch(
                $this->protect_identifiers( $table, TRUE, $escape, FALSE ),
                $this->_keys, array_slice( $this->_sets, $i, 100 )
            ) );

            $affected_rows += $this->affected_rows();
        }

        $this->_reset_write();

        return $affected_rows;
    }

    // --------------------------------------------------------------------

    /**
     * The "set_insert_batch" function.  Allows key/value pairs to be set for batch inserts
     *
     * @param   mixed  $keys
     * @param   string $value
     * @param   bool   $escape
     *
     * @return  \O2System\O2DB\Interfaces\Query
     */
    public function set_insert_batch( $keys, $value = '', $escape = NULL )
    {
        $keys = $this->_object_to_array_batch( $keys );

        if( ! is_array( $keys ) )
        {
            $keys = array( $keys => $value );
        }

        is_bool( $escape ) OR $escape = $this->_protect_identifiers;

        $keys = array_keys( $this->_object_to_array( current( $keys ) ) );
        sort( $keys );

        foreach( $keys as $row )
        {
            $row = $this->_object_to_array( $row );
            if( count( array_diff( $keys, array_keys( $row ) ) ) > 0 OR
                count( array_diff( array_keys( $row ), $keys ) ) > 0
            )
            {
                // batch function above returns an error on an empty array
                $this->_sets[ ] = array();

                return;
            }

            ksort( $row ); // puts $row in the same order as our keys

            if( $escape !== FALSE )
            {
                $clean = array();
                foreach( $row as $value )
                {
                    $clean[ ] = $this->escape( $value );
                }

                $row = $clean;
            }

            $this->_sets[ ] = '(' . implode( ',', $row ) . ')';
        }

        foreach( $keys as $key )
        {
            $this->_keys[ ] = $this->protect_identifiers( $key, FALSE, $escape );
        }

        return $this;
    }

    // --------------------------------------------------------------------

    /**
     * Object to Array
     *
     * Takes an object as input and converts the class variables to array key/vals
     *
     * @param    object
     *
     * @return    array
     */
    protected function _object_to_array_batch( $object )
    {
        if( ! is_object( $object ) )
        {
            return $object;
        }

        $array = array();
        $out = get_object_vars( $object );
        $fields = array_keys( $out );

        foreach( $fields as $val )
        {
            // There are some built in keys we need to ignore for this conversion
            if( $val !== '_parent_name' )
            {
                $i = 0;
                foreach( $out[ $val ] as $data )
                {
                    $array[ $i++ ][ $val ] = $data;
                }
            }
        }

        return $array;
    }

    // --------------------------------------------------------------------

    /**
     * Object to Array
     *
     * Takes an object as input and converts the class variables to array key/vals
     *
     * @param    object
     *
     * @return    array
     */
    protected function _object_to_array( $object )
    {
        if( ! is_object( $object ) )
        {
            return $object;
        }

        $array = array();
        foreach( get_object_vars( $object ) as $key => $val )
        {
            // There are some built in keys we need to ignore for this conversion
            if( ! is_object( $val ) && ! is_array( $val ) && $key !== '_parent_name' )
            {
                $array[ $key ] = $val;
            }
        }

        return $array;
    }

    // --------------------------------------------------------------------

    /**
     * Insert batch statement
     *
     * Generates a platform-specific insert string from the supplied data.
     *
     * @param    string $table  Table name
     * @param    array  $keys   INSERT keys
     * @param    array  $values INSERT values
     *
     * @return    string
     */
    protected function _insert_batch( $table, $keys, $values )
    {
        return 'INSERT INTO ' . $table . ' (' . implode( ', ', $keys ) . ') VALUES ' . implode( ', ', $values );
    }

    // --------------------------------------------------------------------

    /**
     * Resets the query builder "write" values.
     *
     * Called by the insert() update() insert_batch() update_batch() and delete() functions
     *
     * @return    void
     */
    protected function _reset_write()
    {
        $this->_reset_run( array(
                               '_sets'     => array(),
                               '_from'     => array(),
                               '_join'     => array(),
                               '_where'    => array(),
                               '_order_by' => array(),
                               '_keys'     => array(),
                               '_limit'    => FALSE
                           ) );
    }

    // --------------------------------------------------------------------

    /**
     * Get INSERT query string
     *
     * Compiles an insert query and returns the sql
     *
     * @param    string    the table to insert into
     * @param    bool      TRUE: reset QB values; FALSE: leave QB values alone
     *
     * @return    string
     */
    public function get_compiled_insert( $table = '', $reset = TRUE )
    {
        if( $this->_validate_insert( $table ) === FALSE )
        {
            return FALSE;
        }

        $sql = $this->_insert(
            $this->protect_identifiers( $this->_from[ 0 ], TRUE, NULL, FALSE ),
            array_keys( $this->_sets ),
            array_values( $this->_sets )
        );

        if( $reset === TRUE )
        {
            $this->_reset_write();
        }

        return $sql;
    }

    // --------------------------------------------------------------------

    /**
     * Validate Insert
     *
     * This method is used by both insert() and get_compiled_insert() to
     * validate that the there data is actually being set and that table
     * has been chosen to be inserted into.
     *
     * @param   string $table the table to insert data into
     *
     * @return  string
     * @throws  \Exception
     */
    protected function _validate_insert( $table = '' )
    {
        if( count( $this->_sets ) === 0 )
        {
            if( $this->debug_enabled )
            {
                throw new \Exception( 'You must use the "set" method to update an entry.' );
            }

            return FALSE;
        }

        if( $table !== '' )
        {
            $this->_from[ 0 ] = $table;
        }
        elseif( ! isset( $this->_from[ 0 ] ) )
        {
            if( $this->debug_enabled )
            {
                throw new \Exception( 'You must set the database table to be used with your query.' );
            }

            return FALSE;
        }

        return TRUE;
    }

    // --------------------------------------------------------------------

    /**
     * Insert
     *
     * Compiles an insert string and runs the query
     *
     * @param   string $table  the table to insert data into
     * @param   array  $set    an associative array of insert values
     * @param   bool   $escape Whether to escape values and identifiers
     *
     * @return  object
     */
    public function insert( $table = '', $set = NULL, $escape = NULL )
    {
        if( $set !== NULL )
        {
            $this->set( $set, '', $escape );
        }

        if( $this->_validate_insert( $table ) === FALSE )
        {
            return FALSE;
        }

        $sql = $this->_insert(
            $this->protect_identifiers( $this->_from[ 0 ], TRUE, $escape, FALSE ),
            array_keys( $this->_sets ),
            array_values( $this->_sets )
        );

        $this->_reset_write();

        return $this->query( $sql );
    }

    // --------------------------------------------------------------------

    /**
     * The "set" function.
     *
     * Allows key/value pairs to be set for inserting or updating
     *
     * @param   mixed  $key
     * @param   string $value
     * @param   bool   $escape
     *
     * @return  \O2System\O2DB\Interfaces\Query
     */
    public function set( $key, $value = '', $escape = NULL )
    {
        $key = $this->_object_to_array( $key );

        if( ! is_array( $key ) )
        {
            $key = array( $key => $value );
        }

        is_bool( $escape ) OR $escape = $this->_protect_identifiers;

        foreach( $key as $k => $v )
        {
            $this->_sets[ $this->protect_identifiers( $k, FALSE, $escape ) ] = ( $escape )
                ? $this->escape( $v ) : $v;
        }

        return $this;
    }

    // --------------------------------------------------------------------

    /**
     * Replace
     *
     * Compiles an replace into string and runs the query
     *
     * @param   string  the table to replace data into
     * @param   array   an associative array of insert values
     *
     * @return  object
     * @throws  \Exception
     */
    public function replace( $table = '', $set = NULL )
    {
        if( $set !== NULL )
        {
            $this->set( $set );
        }

        if( count( $this->_sets ) === 0 )
        {
            if( $this->debug_enabled )
            {
                throw new \Exception( 'You must use the "set" method to update an entry.' );
            }

            return FALSE;
        }

        if( $table === '' )
        {
            if( ! isset( $this->_from[ 0 ] ) )
            {
                if( $this->debug_enabled )
                {
                    throw new \Exception( 'You must set the database table to be used with your query.' );
                }

                return FALSE;
            }

            $table = $this->_from[ 0 ];
        }

        $sql = $this->_replace(
            $this->protect_identifiers( $table, TRUE, NULL, FALSE ),
            array_keys( $this->_sets ),
            array_values( $this->_sets )
        );

        $this->_reset_write();

        return $this->query( $sql );
    }

    // --------------------------------------------------------------------

    /**
     * Replace statement
     *
     * Generates a platform-specific replace string from the supplied data
     *
     * @param   string  the table name
     * @param   array   the insert keys
     * @param   array   the insert values
     *
     * @return  string
     */
    protected function _replace( $table, $keys, $values )
    {
        return 'REPLACE INTO ' . $table . ' (' . implode( ', ', $keys ) . ') VALUES (' . implode( ', ', $values ) . ')';
    }

    // --------------------------------------------------------------------

    /**
     * Get UPDATE query string
     *
     * Compiles an update query and returns the sql
     *
     * @param   string  the table to update
     * @param   bool    TRUE: reset QB values; FALSE: leave QB values alone
     *
     * @return  string
     */
    public function get_compiled_update( $table = '', $reset = TRUE )
    {
        // Combine any cached components with the current statements
        $this->_merge_cache();

        if( $this->_validate_update( $table ) === FALSE )
        {
            return FALSE;
        }

        $sql = $this->_update(
            $this->protect_identifiers( $this->_from[ 0 ], TRUE, NULL, FALSE ),
            $this->_sets
        );

        if( $reset === TRUE )
        {
            $this->_reset_write();
        }

        return $sql;
    }

    // --------------------------------------------------------------------

    /**
     * Validate Update
     *
     * This method is used by both update() and get_compiled_update() to
     * validate that data is actually being set and that a table has been
     * chosen to be update.
     *
     * @param   string $table the table to update data on
     *
     * @return  bool
     * @throws  \Exception
     */
    protected function _validate_update( $table = '' )
    {
        if( count( $this->_sets ) === 0 )
        {
            if( $this->debug_enabled )
            {
                throw new \Exception( 'You must use the "set" method to update an entry.' );
            }

            return FALSE;
        }

        if( $table !== '' )
        {
            $this->_from[ 0 ] = $table;
        }
        elseif( ! isset( $this->_from[ 0 ] ) )
        {
            if( $this->debug_enabled )
            {
                throw new \Exception( 'You must set the database table to be used with your query.' );
            }

            return FALSE;
        }

        return TRUE;
    }

    // --------------------------------------------------------------------

    /**
     * UPDATE
     *
     * Compiles an update string and runs the query.
     *
     * @param   string $table
     * @param   array  $set An associative array of update values
     * @param   mixed  $where
     * @param   int    $limit
     *
     * @return  object
     * @throws  \Exception
     */
    public function update( $table = '', $set = NULL, $where = NULL, $limit = NULL )
    {
        // Combine any cached components with the current statements
        $this->_merge_cache();

        if( $set !== NULL )
        {
            $this->set( $set );
        }

        if( $this->_validate_update( $table ) === FALSE )
        {
            return FALSE;
        }

        if( $where !== NULL )
        {
            $this->where( $where );
        }

        if( ! empty( $limit ) )
        {
            $this->limit( $limit );
        }

        $sql = $this->_update(
            $this->protect_identifiers( $this->_from[ 0 ], TRUE, NULL, FALSE ),
            $this->_sets
        );

        $this->_reset_write();

        return $this->query( $sql );
    }

    // --------------------------------------------------------------------

    /**
     * Update_Batch
     *
     * Compiles an update string and runs the query
     *
     * @param string $table
     * @param null   $set
     * @param null   $index
     *
     * @return int number of rows affected or FALSE on failure
     * @throws \Exception
     */
    public function update_batch( $table = '', $set = NULL, $index = NULL )
    {
        // Combine any cached components with the current statements
        $this->_merge_cache();

        if( $index === NULL )
        {
            if( $this->debug_enabled )
            {
                throw new \Exception( 'You must specify an index to match on for batch updates.' );
            }

            return FALSE;
        }

        if( $set !== NULL )
        {
            $this->set_update_batch( $set, $index );
        }

        if( count( $this->_sets ) === 0 )
        {
            if( $this->debug_enabled )
            {
                throw new \Exception( 'You must use the "set" method to update an entry.' );
            }

            return FALSE;
        }

        if( $table === '' )
        {
            if( ! isset( $this->_from[ 0 ] ) )
            {
                if( $this->debug_enabled )
                {
                    throw new \Exception( 'You must set the database table to be used with your query.' );
                }

                return FALSE;
            }

            $table = $this->_from[ 0 ];
        }

        // Batch this baby
        $affected_rows = 0;

        for( $i = 0, $total = count( $this->_sets ); $i < $total; $i += 100 )
        {
            $this->query( $this->_update_batch(
                $this->protect_identifiers( $table, TRUE, NULL, FALSE ),
                array_slice( $this->_sets, $i, 100 ),
                $this->protect_identifiers( $index )
            ) );

            $affected_rows += $this->affected_rows();

            $this->_where = array();
        }

        $this->_reset_write();

        return $affected_rows;
    }

    // --------------------------------------------------------------------

    /**
     * The "set_update_batch" function.  Allows key/value pairs to be set for batch updating
     *
     * @param        $keys
     * @param string $index
     * @param null   $escape
     *
     * @return Query
     * @throws \Exception
     */
    public function set_update_batch( $keys, $index = '', $escape = NULL )
    {
        $keys = $this->_object_to_array_batch( $keys );

        if( ! is_array( $keys ) )
        {
            // @todo error
        }

        is_bool( $escape ) OR $escape = $this->_protect_identifiers;

        foreach( $keys as $key => $value )
        {
            $index_set = FALSE;
            $clean = array();
            foreach( $value as $k => $v )
            {
                if( $k === $index )
                {
                    $index_set = TRUE;
                }

                $clean[ $this->protect_identifiers( $k, FALSE,
                                                    $escape ) ] = ( $escape === FALSE ) ? $v : $this->escape( $v );
            }

            if( $index_set === FALSE )
            {
                throw new \Exception( 'You must specify an index to match on for batch updates.' );
            }

            $this->_sets[ ] = $clean;
        }

        return $this;
    }

    // --------------------------------------------------------------------

    /**
     * Update_Batch statement
     *
     * Generates a platform-specific batch update string from the supplied data
     *
     * @param    string $table  Table name
     * @param    array  $values Update data
     * @param    string $index  WHERE key
     *
     * @return    string
     */
    protected function _update_batch( $table, $values, $index )
    {
        $ids = array();
        foreach( $values as $key => $value )
        {
            $ids[ ] = $value[ $index ];

            foreach( array_keys( $value ) as $field )
            {
                if( $field !== $index )
                {
                    $final[ $field ][ ] = 'WHEN ' . $index . ' = ' . $value[ $index ] . ' THEN ' . $value[ $field ];
                }
            }
        }

        $cases = '';
        foreach( $final as $key => $value )
        {
            $cases .= $key . " = CASE \n"
                      . implode( "\n", $value ) . "\n"
                      . 'ELSE ' . $key . ' END, ';
        }

        $this->where( $index . ' IN(' . implode( ',', $ids ) . ')', NULL, FALSE );

        return 'UPDATE ' . $table . ' SET ' . substr( $cases, 0, -2 ) . $this->_compile_where( '_where' );
    }

    // --------------------------------------------------------------------

    /**
     * Empty Table
     *
     * Compiles a delete string and runs "DELETE FROM table"
     *
     * @param string $table the table to empty
     *
     * @return object
     * @throws \Exception
     */
    public function empty_table( $table = '' )
    {
        if( $table === '' )
        {
            if( ! isset( $this->_from[ 0 ] ) )
            {
                if( $this->debug_enabled )
                {
                    throw new \Exception( 'You must set the database table to be used with your query.' );
                }

                return FALSE;
            }

            $table = $this->_from[ 0 ];
        }
        else
        {
            $table = $this->protect_identifiers( $table, TRUE, NULL, FALSE );
        }

        $sql = $this->_delete( $table );
        $this->_reset_write();

        return $this->query( $sql );
    }

    // --------------------------------------------------------------------

    /**
     * Delete statement
     *
     * Generates a platform-specific delete string from the supplied data
     *
     * @param    string    the table name
     *
     * @return    string
     */
    protected function _delete( $table )
    {
        return 'DELETE FROM ' . $table . $this->_compile_where( '_where' ) . ( $this->_limit ? ' LIMIT ' . $this->_limit : '' );
    }

    // --------------------------------------------------------------------

    /**
     * Truncate
     *
     * Compiles a truncate string and runs the query
     * If the database does not support the truncate() command
     * This function maps to "DELETE FROM table"
     *
     * @param string $table the table to truncate
     *
     * @return object
     * @throws \Exception
     */
    public function truncate( $table = '' )
    {
        if( $table === '' )
        {
            if( ! isset( $this->_from[ 0 ] ) )
            {
                if( $this->debug_enabled )
                {
                    throw new \Exception( 'You must set the database table to be used with your query.' );
                }

                return FALSE;
            }

            $table = $this->_from[ 0 ];
        }
        else
        {
            $table = $this->protect_identifiers( $table, TRUE, NULL, FALSE );
        }

        $sql = $this->_truncate( $table );
        $this->_reset_write();

        return $this->query( $sql );
    }

    // --------------------------------------------------------------------

    /**
     * Truncate statement
     *
     * Generates a platform-specific truncate string from the supplied data
     *
     * If the database does not support the truncate() command,
     * then this method maps to 'DELETE FROM table'
     *
     * @param    string    the table name
     *
     * @return    string
     */
    protected function _truncate( $table )
    {
        return 'TRUNCATE ' . $table;
    }

    // --------------------------------------------------------------------

    /**
     * Get DELETE query string
     *
     * Compiles a delete query string and returns the sql
     *
     * @param    string    the table to delete from
     * @param    bool      TRUE: reset QB values; FALSE: leave QB values alone
     *
     * @return    string
     */
    public function get_compiled_delete( $table = '', $reset = TRUE )
    {
        $this->return_delete_sql = TRUE;
        $sql = $this->delete( $table, '', NULL, $reset );
        $this->return_delete_sql = FALSE;

        return $sql;
    }

    // --------------------------------------------------------------------

    /**
     * Delete
     *
     * Compiles a delete string and runs the query
     *
     * @param string|array $table table(s) to delete from. String or array
     * @param string       $where Where Clause
     * @param null         $limit Limit Clause
     * @param bool         $reset_data
     *
     * @return mixed
     * @throws \Exception
     */
    public function delete( $table = '', $where = '', $limit = NULL, $reset_data = TRUE )
    {
        // Combine any cached components with the current statements
        $this->_merge_cache();

        if( $table === '' )
        {
            if( ! isset( $this->_from[ 0 ] ) )
            {
                if( $this->debug_enabled )
                {
                    throw new \Exception( 'You must set the database table to be used with your query.' );
                }

                return FALSE;
            }

            $table = $this->_from[ 0 ];
        }
        elseif( is_array( $table ) )
        {
            foreach( $table as $single_table )
            {
                $this->delete( $single_table, $where, $limit, $reset_data );
            }

            return;
        }
        else
        {
            $table = $this->protect_identifiers( $table, TRUE, NULL, FALSE );
        }

        if( $where !== '' )
        {
            $this->where( $where );
        }

        if( ! empty( $limit ) )
        {
            $this->limit( $limit );
        }

        if( count( $this->_where ) === 0 )
        {
            if( $this->debug_enabled )
            {
                throw new \Exception( 'Deletes are not allowed unless they contain a "where" or "like" clause.' );
            }

            return FALSE;
        }

        $sql = $this->_delete( $table );

        if( $reset_data )
        {
            $this->_reset_write();
        }

        return ( $this->return_delete_sql === TRUE ) ? $sql : $this->query( $sql );
    }

    // --------------------------------------------------------------------

    /**
     * DB Prefix
     *
     * Prepends a database prefix if one exists in configuration
     *
     * @param string $table the table
     *
     * @return string
     * @throws \Exception
     */
    public function prefix_table( $table = '' )
    {
        if( $table === '' )
        {
            throw new \Exception( 'A table name is required for that operation.' );
        }

        return $this->prefix_table . $table;
    }

    // --------------------------------------------------------------------

    /**
     * Set DB Prefix
     *
     * Set's the DB Prefix to something new without needing to reconnect
     *
     * @param    string    the prefix
     *
     * @return    string
     */
    public function set_table_prefix( $prefix = '' )
    {
        return $this->prefix_table = $prefix;
    }

    // --------------------------------------------------------------------

    /**
     * Start Cache
     *
     * Starts QB caching
     *
     * @return    \O2System\O2DB\Interfaces\Query
     */
    public function start_cache()
    {
        $this->_caching = TRUE;

        return $this;
    }

    // --------------------------------------------------------------------

    /**
     * Stop Cache
     *
     * Stops QB caching
     *
     * @return    \O2System\O2DB\Interfaces\Query
     */
    public function stop_cache()
    {
        $this->_caching = FALSE;

        return $this;
    }

    // --------------------------------------------------------------------

    /**
     * Flush Cache
     *
     * Empties the QB cache
     *
     * @return    \O2System\O2DB\Interfaces\Query
     */
    public function flush_cache()
    {
        $this->_reset_run( array(
                               '_cache_select'    => array(),
                               '_cache_from'      => array(),
                               '_cache_join'      => array(),
                               '_cache_where'     => array(),
                               '_cache_group_by'  => array(),
                               '_cache_having'    => array(),
                               '_cache_order_by'  => array(),
                               '_cache_sets'      => array(),
                               '_cache_exists'    => array(),
                               '_cache_no_escape' => array()
                           ) );

        return $this;
    }

    // --------------------------------------------------------------------

    /**
     * Reset Query Builder values.
     *
     * Publicly-visible method to reset the QB values.
     *
     * @return    \O2System\O2DB\Interfaces\Query
     */
    public function reset_query()
    {
        $this->_reset_select();
        $this->_reset_write();

        return $this;
    }

}
