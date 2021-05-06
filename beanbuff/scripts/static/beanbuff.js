// Bind SLASH to focus on the search box of the DataTable instance from
// anywhere.
function SetDataTableFocus() {
    $(document).keypress(function(ev) {
        var search = $(".dataTables_filter input");
        if (ev.which == 47 && ev.key == '/')  {
            if (!search.is(":focus")) {
                event.preventDefault();
                search.focus();
            }
        }
    });
}
