// Bind SLASH to focus on the search box of the DataTable instance from
// anywhere.
function InstallDataTableFocus() {
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

// // Find a column index by header name.
// function FindColumn(table, name) {
//     var header = table.table().header()
//     return $(header).find('th:contains("' + name + '")').index();
// }

// function SelectedChains(table) {
//     var index = FindColumn(table, "chain_id");
//     return table.rows('.selected').data();
// }
