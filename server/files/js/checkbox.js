function FillCheckBox(element) {
    val = $('#datasets_ids').val();

    if (val) {
        chosed_vals = new Set(val.split(','));
    } else {
        chosed_vals = new Set();
    }

    if (element.checked) {
        chosed_vals.add($(element).attr('id'));
    } else {
        chosed_vals.delete($(element).attr('id'));
    }

    list = Array.from(chosed_vals);
    $('#datasets_ids').val(list.join(','));
}
