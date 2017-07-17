// leave at least 2 line with only a star on it below, or doc generation fails
/**
 *
 *
 * Placeholder for custom user javascript
 * mainly to be overridden in profile/static/custom/custom.js
 * This will always be an empty file in IPython
 *
 * User could add any javascript in the `profile/static/custom/custom.js` file.
 * It will be executed by the ipython notebook at load time.
 *
 * Same thing with `profile/static/custom/custom.css` to inject custom css into the notebook.
 *
 *
 * The object available at load time depend on the version of IPython in use.
 * there is no guaranties of API stability.
 *
 * The example below explain the principle, and might not be valid.
 *
 * Instances are created after the loading of this file and might need to be accessed using events:
 *     define([
 *        'base/js/namespace',
 *        'base/js/events'
 *     ], function(IPython, events) {
 *         events.on("app_initialized.NotebookApp", function () {
 *             IPython.keyboard_manager....
 *         });
 *     });
 *
 * __Example 1:__
 *
 * Create a custom button in toolbar that execute `%qtconsole` in kernel
 * and hence open a qtconsole attached to the same kernel as the current notebook
 *
 *    define([
 *        'base/js/namespace',
 *        'base/js/events'
 *    ], function(IPython, events) {
 *        events.on('app_initialized.NotebookApp', function(){
 *            IPython.toolbar.add_buttons_group([
 *                {
 *                    'label'   : 'run qtconsole',
 *                    'icon'    : 'icon-terminal', // select your icon from http://fortawesome.github.io/Font-Awesome/icons
 *                    'callback': function () {
 *                        IPython.notebook.kernel.execute('%qtconsole')
 *                    }
 *                }
 *                // add more button here if needed.
 *                ]);
 *        });
 *    });
 *
 * __Example 2:__
 *
 * At the completion of the dashboard loading, load an unofficial javascript extension
 * that is installed in profile/static/custom/
 *
 *    define([
 *        'base/js/events'
 *    ], function(events) {
 *        events.on('app_initialized.DashboardApp', function(){
 *            require(['custom/unofficial_extension.js'])
 *        });
 *    });
 *
 * __Example 3:__
 *
 *  Use `jQuery.getScript(url [, success(script, textStatus, jqXHR)] );`
 *  to load custom script into the notebook.
 *
 *    // to load the metadata ui extension example.
 *    $.getScript('/static/notebook/js/celltoolbarpresets/example.js');
 *    // or
 *    // to load the metadata ui extension to control slideshow mode / reveal js for nbconvert
 *    $.getScript('/static/notebook/js/celltoolbarpresets/slideshow.js');
 *
 *
 * @module IPython
 * @namespace IPython
 * @class customjs
 * @static
 */

IPython._target = '_self';

define([
    'jquery',
    'base/js/events'
], function(
    $,
    events
    ) {
    "use strict";

    // Hide some dashboard features from the user.
    events.on('app_initialized.DashboardApp', function() {
        $('#tabs li > a[href="#clusters"]').parent().css('display', 'none');

        // Helper to fix the new notebook link text.
        function fix_notebook_name() {
            return $('li#kernel-python2 > a').text('Notebook').length !== 0;
        }

        // Attempt to fix the link text.
        if (!fix_notebook_name()) {

            // If the notebook anchor is not present, fix the name once it has
            // been added.
            var observer = new MutationObserver(function() {
                if (fix_notebook_name()) {
                    observer.disconnect();
                }
            });
            observer.observe($('ul#new-menu').get(0), { childList: true });
        }
    });

    // Notebook customizations.
    events.on('app_initialized.NotebookApp', function() {
        // Hide the notebook cells on startup, disabling interaction until the
        // kernel is loaded.
        $('#notebook-container').addClass('hidden-during-load');

        // Helper to insert a link to epidata documentation.
        function add_epidata_help() {
            return $('#kernel-help-links').after($('<li>' +
                '<a target="_blank" title="Opens in a new window" href="/assets/doc/index.html">' +
                '<i class="fa fa-external-link menu-icon pull-right">' +
                '</i><span>EpiData</span></a></li>')).length;
        }

        // Attempt to add the help link.
        if (!add_epidata_help()) {

            // If the kernel help links section is not present, add the link
            // once the section has been added.
            var observer = new MutationObserver(function() {
                if (add_epidata_help()) {
                    observer.disconnect();
                }
            });
            observer.observe($('ul#help_menu').get(0), { childList: true });
        }

        // Override 'close and halt' menu option to redirect back to tree view.
        $('#kill_and_exit').unbind('click').click(function () {
            var redirect = function () {
                window.open('/user/tree', '_self');
            };
            // Finish with redirect on success or failure.
            IPython.notebook.session.delete(redirect, redirect);
        });
    });

    // Customizations on loading a notebook.
    events.on('notebook_loaded.Notebook', function() {
        // Prevent editing read only notebooks.
        if (IPython.notebook.metadata.read_only) {
            // The IPython.notebook has been sealed, so disable command mode
            // functionality using the prototype chain.
            IPython.Notebook.command_mode = function() {};
            IPython.Notebook.handle_command_mode = function() {};
            IPython.Cell.prototype.command_mode = function() {};
            IPython.Cell.prototype.unrender = function() {};
        }
    });

    // Once the kernel is loaded, show the notebook cells.
    events.on('kernel_connected.Kernel', function () {
        $('#notebook-container').removeClass('hidden-during-load');
    });

    // To implement a template for new notebooks, adjust the notebook cells
    // on startup. A new notebook is identified as a notebook with only a
    // single, empty code cell.
    events.one('app_initialized.NotebookApp', function() {
        events.one('create.Cell', function(event, created) {

            var cell = created.cell;

            var apply_template = function(cm, change) {
                cell.code_mirror.off('update', apply_template);

                if (IPython.notebook.get_cells().length != 1 ||
                    cell.cell_type != 'code' ||
                    cell.get_text() != '') {
                    // Not a newly initialized notebook, so don't apply template.
                    return;
                }

                // The notebook is a new notebook (or has the same contents as
                // a newly initialized notebook), so apply the template.

                // Add text to the first cell.
                cell.set_text('# from epidata.context import ec');

                // Add a second cell.
                IPython.notebook.insert_cell_at_index('code', 1);
            }

            cell.code_mirror.on('update', apply_template);
        });
    });

});
