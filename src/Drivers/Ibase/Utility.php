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
namespace O2System\O2DB\Drivers\iBase;
defined( 'BASEPATH' ) OR exit( 'No direct script access allowed' );

/**
 * Database Driver Class
 *
 * @package        O2System
 * @subpackage     Drivers
 * @category       Database
 * @author         Steeven Andrian Salim
 * @link           http://o2system.center/framework/user-guide/libraries/database.htm
 */
class Utility extends \O2System\O2DB\Interfaces\Utility
{

    /**
     * Export
     *
     * @param    string $filename
     *
     * @return    mixed
     */
    protected function _backup( $filename )
    {
        if( $service = ibase_service_attach( $this->db->hostname, $this->db->username, $this->db->password ) )
        {
            $res = ibase_backup( $service, $this->db->database, $filename . '.fbk' );

            // Close the service connection
            ibase_service_detach( $service );

            return $res;
        }

        return FALSE;
    }

}

/* End of file Utility.php */
/* Location: ./system/libraries/database/drivers/iBase/Utility.php */
