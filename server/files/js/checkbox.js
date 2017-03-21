function FillCheckBox(element){
    if (element.checked) {
        chosed_vals.add($(element).attr('id'));
    } else {
        chosed_vals.delete($(element).attr('id'));
    }
}


$(document).ready(function(){
    chosed_vals = new Set($('#datasets_ids').val().split(','));
    $('#submit_button').bind("click", function() {
        list = Array.from(chosed_vals);
        $('#datasets_ids').val(list.join(','));
    });
});
