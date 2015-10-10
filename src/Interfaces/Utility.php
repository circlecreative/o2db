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

namespace O2System\DB\Interfaces;

// ------------------------------------------------------------------------

/**
 * Utility Interface Class
 *
 * @package     O2DB
 * @subpackage  Interfaces
 * @category    Interface Class
 * @author      Circle Creative Developer Team
 * @link        http://circle-creative.com/products/o2db.html
 */
abstract class Utility
{
    /**
     * Connection Class Object
     *
     * @access  protected
     * @type    Connection
     */
    protected $_conn;

    // ------------------------------------------------------------------------

    /**
     * Class Constructor
     *
     * @param Connection $connection
     *
     * @access  public
     */
    public function __construct( Connection &$connection )
    {
        $this->_conn =& $connection;
    }

    // ------------------------------------------------------------------------

    /**
     * Optimize Table
     *
     * Optimize database table
     *
     * @param   string  $table  Database table name
     *
     * @access  public
     * @return  mixed
     */
    abstract public function optimize_table($table);

    // ------------------------------------------------------------------------

    /**
     * Repair Table
     *
     * Repair database table
     *
     * @param   string  $table  Database table name
     *
     * @access  public
     * @return  mixed
     */
    abstract public function repair_table($table);
}
