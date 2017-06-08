$(document).ready(function() {
    var $TABLE = $('#table');
    var $BTN = $('#export-btn');
    var $EXPORT = $('#export');
    var id = 0;


    $('.table-add').click(function () {
        var $clone = $TABLE.find('tr.hide').clone(true).removeClass('hide table-line').addClass('active').attr('id', 'new_row_' + id);;
        id += 1;
        $TABLE.find('table').append($clone);
    });

    $('.btn-success').click(function () {
        var $clone = $TABLE.find('tr.hide').clone(true).removeClass('hide table-line').addClass('active').attr('id', 'new_row_' + id);;
        id += 1;
        $TABLE.find('table').append($clone);
    });


    $('.table-remove').click(function () {
        $(this).parents('tr').detach();
    });

    $('.table-up').click(function () {
        var $row = $(this).parents('tr');
        if ($row.index() === 1) return; // Don't go above the header
        $row.prev().before($row.get(0));
    });

    $('.table-down').click(function () {
        var $row = $(this).parents('tr');
        $row.next().after($row.get(0));
    });

    // A few jQuery helpers for exporting only
    jQuery.fn.pop = [].pop;
    jQuery.fn.shift = [].shift;

    $BTN.click(function () {
        var new_rows = []

        for (var i = 0; i < id; i += 1) {
            row = $('#new_row_1');
            $row.each(function () {
                new_rows += [$(this.text())]
            })
        }
      $EXPORT.text(JSON.stringify(data));
    });
});
