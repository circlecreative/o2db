<?php
/**
 * O2ORM
 *
 * An open source ORM Database Framework for PHP 5.2.4 or newer
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
 * @license     http://circle-creative.com/products/o2system/license.html
 * @license     http://opensource.org/licenses/MIT  MIT License
 * @link        http://circle-creative.com
 * @since       Version 1.0
 * @filesource
 */
// ------------------------------------------------------------------------

namespace O2System\DB\Metadata;

/**
 * Result Metadata Class
 *
 * @package     O2DB
 * @subpackage  Metadata
 * @category    Metadata Class
 * @author      Circle Creative Developer Team
 */
class Result
{
    public function __construct( $data = array() )
    {
        if( ! empty( $data ) )
        {
            foreach( $data as $key => $value )
            {
                $this->{$key} = $value;
            }
        }
    }

    // ------------------------------------------------------------------------

    public function __set( $name, $value )
    {
        if( is_string( $value ) )
        {
            if( $this->_is_serialize( $value ) )
            {
                $value = unserialize( $value );
            }
            elseif( $this->_is_json( $value ) )
            {
                $value = json_decode( $value, TRUE );
            }
        }

        if( is_array( $value ) )
        {
            $key = array_keys($value);
            $key = reset($key);

            if(is_numeric($key))
            {
                $this->{$name} = $value;
            }
            else
            {
                $this->{$name} = new Result( $value );
            }
        }
        else
        {
            $this->{$name} = $value;
        }
    }

    // ------------------------------------------------------------------------

    protected function _is_serialize( $string )
    {
        // Bit of a give away this one
        if( ! is_string( $string ) )
        {
            return FALSE;
        }

        // Serialized false, return true. unserialize() returns false on an
        // invalid string or it could return false if the string is serialized
        // false, eliminate that possibility.
        if( $string === 'b:0;' )
        {
            return TRUE;
        }

        $length = strlen( $string );
        $end = '';

        if( ! isset( $string[ 0 ] ) ) return FALSE;

        switch( $string[ 0 ] )
        {
            case 's':
                if( $string[ $length - 2 ] !== '"' )
                {
                    return FALSE;
                }
            case 'b':
            case 'i':
            case 'd':
                // This looks odd but it is quicker than isset()ing
                $end .= ';';
            case 'a':
            case 'O':
                $end .= '}';

                if( $string[ 1 ] !== ':' )
                {
                    return FALSE;
                }

                switch( $string[ 2 ] )
                {
                    case 0:
                    case 1:
                    case 2:
                    case 3:
                    case 4:
                    case 5:
                    case 6:
                    case 7:
                    case 8:
                    case 9:
                        break;

                    default:
                        return FALSE;
                }
            case 'N':
                $end .= ';';

                if( $string[ $length - 1 ] !== $end[ 0 ] )
                {
                    return FALSE;
                }
                break;

            default:
                return FALSE;
        }

        return (bool)unserialize( $string );
    }

    // ------------------------------------------------------------------------

    protected function _is_json( $string )
    {
        // make sure provided input is of type string
        if( ! is_string( $string ) )
        {
            return FALSE;
        }

        // trim white spaces
        $string = trim( $string );

        // get first character
        $first_char = substr( $string, 0, 1 );

        // get last character
        $last_char = substr( $string, -1 );

        // check if there is a first and last character
        if( ! $first_char || ! $last_char )
        {
            return FALSE;
        }

        // make sure first character is either { or [
        if( $first_char !== '{' && $first_char !== '[' )
        {
            return FALSE;
        }

        // make sure last character is either } or ]
        if( $last_char !== '}' && $last_char !== ']' )
        {
            return FALSE;
        }

        // let's leave the rest to PHP.
        // try to decode string
        json_decode( $string );

        // check if error occurred
        if( json_last_error() === JSON_ERROR_NONE )
        {
            return TRUE;
        }

        return FALSE;
    }

    // ------------------------------------------------------------------------

    public function __toString()
    {
        return json_encode( $this );
    }

    // ------------------------------------------------------------------------

    public function __toArray()
    {
        return json_decode( $this->__toString(), TRUE );
    }
}