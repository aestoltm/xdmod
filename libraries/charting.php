<?php

namespace xd_charting;

function processForReport(&$chart_config)
{
    $data = json_encode($chart_config);
    $chart_config = json_decode($data, true);

    if ( isset($chart_config['data']) && isset($chart_config['data'][0]) ) {
        $chart_config = $chart_config['data'][0];
    }

    $chart_config['layout']['annotations'][2] = '';
}

function processForThumbnail(&$chart_config)
{
    if ( isset($chart_config['data']) && isset($chart_config['data'][0]) ) {
        $chart_config = $chart_config['data'][0];
    }

    $chart_config['layout']['showlegend'] = false;

    $chart_config['layout']['annotations'][2]['text'] = '';

    $chart_config['layout']['annotations'][0]['text'] = '';
    $chart_config['layout']['annotations'][1]['text'] = '';

    $chart_config['layout']['xaxis']['title']['text'] = '';
    $chart_config['layout']['xaxis']['showticklabels'] = false;

    $chart_config['layout']['yaxis']['title']['text'] = '';
    $chart_config['layout']['yaxis']['showticklabels'] = false;
}

function exportChart(
    $chartConfig,
    $width,
    $height,
    $scale,
    $format,
    $globalChartConfig = null,
    $fileMetadata = null
) {
    $effectiveWidth = (int)($width*$scale);
    $effectiveHeight = (int)($height*$scale);

    $html_dir = __DIR__ . "/../html";
    $template = file_get_contents($html_dir . "/plotly_template.html");

    $template = str_replace('_html_dir_', $html_dir, $template);
    $template = str_replace('_width_', $effectiveWidth, $template);
    $template = str_replace('_height_', $effectiveHeight, $template);
    $globalChartOptions = array('timezone' => date_default_timezone_get());

    if ($globalChartConfig !== null) {
        $globalChartOptions = array_merge($globalChartOptions, $globalChartConfig);
    }

    $template = str_replace('_globalChartOptions_', json_encode($globalChartOptions), $template);
    $template = str_replace('_chartOptions_', json_encode($chartConfig), $template);
    $svg = getSvgViaChromiumHelper($template, $effectiveWidth, $effectiveHeight);

    switch($format){
        case 'png':
            return convertSvg($svg, 'png', $effectiveWidth, $effectiveHeight, $fileMetadata);
            break;
        case 'pdf':
            return convertSvg($svg, 'pdf', round($width / 90.0 * 72.0), round($height / 90.0 * 72.0), $fileMetadata);
            break;
        default:
            return $svg;
    }
}

/**
 * Use Chromium to generate svg via puppeteer script
 *
 * @param string $html html that should be used by chromium
 * @param int $width desired width of output
 * @param int $height desired height of output
 *
 * @returns string svg
 *
 * @throws \Exception on invalid format, command execution failure, or non zero exit status
 */
function getSvgViaChromiumHelper($html, $width, $height){

    // Chromium requires the file to have a .html extension
    // cant use datauri as it will not execute embdeeded javascript
    $tmpFile = tempnam(sys_get_temp_dir(), 'xdmod-chromiumHtml-');
    $tmpHtmlFile = $tmpFile . '.html';
    if ($tmpFile === false || rename($tmpFile, $tmpHtmlFile) === false) {
        @unlink($tmpFile);
        throw \Exception('Error creating temporary html file for chromium');
    }
    file_put_contents($tmpHtmlFile, $html);

    $chromiumPath = \xd_utilities\getConfiguration('reporting', 'chromium_path');

    $command = LIB_DIR . '/chrome-helper/chrome-helper.js' .
        ' --window-size=' . $width . ',' . $height .
        ' --path-to-chrome=' . $chromiumPath .
        ' --input-file=' . $tmpHtmlFile;

    $pipes = array();
    $descriptor_spec = array(
        1 => array('pipe', 'w'),
        2 => array('pipe', 'w'),
    );
    $process = proc_open($command, $descriptor_spec, $pipes);
    if (!is_resource($process)) {
        @unlink($tmpHtmlFile);
        throw new \Exception('Unable execute command: "'. $command . '". Details: ' . print_r(error_get_last(), true));
    }

    $out = stream_get_contents($pipes[1]);
    $err = stream_get_contents($pipes[2]);
    fclose($pipes[1]);
    fclose($pipes[2]);
    $return_value = proc_close($process);

    @unlink($tmpHtmlFile);

    $chartSvg = json_decode($out);

    if ($chartSvg === null) {
        throw new \Exception('Error executing command: "'. $command . '". Details: ' . $return_value . " " . $out . ' Errors: ' . $err);
    }

    return $chartSvg;
}

/**
 * Use rsvg-convert to convert svg
 *
 * @param string $svgData string of the SVG
 * @param int $width desired width in proper size for format
 * @param int $height desired height in proper size for format
 * @param array $docmeta array containing metadata fields
 *
 * @return string contents of the requested format
 * @throws Exception when unable to execute or non-zero return code
 */

function convertSvg($svgData, $format, $width, $height, $docmeta){
    $author = isset($docmeta['author']) ? addcslashes($docmeta['author'], "()\n\\") : 'XDMoD';
    $subject = isset($docmeta['subject']) ? addcslashes($docmeta['subject'], "()\n\\") : 'XDMoD chart';
    $title = isset($docmeta['title']) ? addcslashes($docmeta['title'], "()\n\\") :'XDMoD PDF chart export';
    $creator = addcslashes('XDMoD ' . OPEN_XDMOD_VERSION, "()\n\\");

    switch($format){
        case 'png':
            $exifArgs = "-Title='$title' -Author='$author' -Description='$subject' -Source='$creator'";
            break;
        case 'pdf':
            $exifArgs = "-Title='$title' -Author='$author' -Subject='$subject' -Creator='$creator'";
            break;
        default:
            return $svgData;
    }

    $rsvgCommand = 'rsvg-convert -w ' .$width. ' -h '.$height.' -f ' . $format;
    if ($format == 'png'){
        $rsvgCommand = 'rsvg-convert -b "#FFFFFF" -w ' .$width. ' -h '.$height.' -f ' . $format;
    }
    $exifCommand = 'exiftool ' . $exifArgs . ' -o - -';

    $command = $rsvgCommand . ' | ' . $exifCommand;
    $pipes = array();
    $descriptor_spec = array(
        0 => array('pipe', 'r'),
        1 => array('pipe', 'w'),
        2 => array('pipe', 'w'),
    );
    $process = proc_open($command, $descriptor_spec, $pipes);
    if (!is_resource($process)) {
        throw new \Exception('Unable execute command: "'. $command . '". Details: ' . print_r(error_get_last(), true));
    }
    fwrite($pipes[0], $svgData);
    fclose($pipes[0]);
    $out = stream_get_contents($pipes[1]);
    $err = stream_get_contents($pipes[2]);

    fclose($pipes[1]);
    fclose($pipes[2]);

    $return_value = proc_close($process);

    if ($return_value != 0) {
        throw new \Exception("$command returned $return_value, stdout: $out stderr: $err");
    }
    return $out;
}
