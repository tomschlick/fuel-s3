<?php

/**
 * FuelPHP package for the Amazon S3 PHP Class
 *
 * @package		Postmark
 * @version		1.0
 * @author		Tom Schlick (tom@tomschlick.com)
 * @link		http://github.com/tomschlick/fuel-s3
 * 
 */

Autoloader::add_core_namespace('S3');

Autoloader::add_classes(array(
	'S3\\S3' => __DIR__.'/classes/s3.php',
	'S3\\S3_Request' => __DIR__.'/classes/s3/request.php',
));