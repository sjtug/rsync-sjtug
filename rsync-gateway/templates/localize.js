function localizeDatetime(e, index, ar) {
    if (e.textContent === undefined) {
        return;
    }
    var d = new Date(e.getAttribute('datetime'));
    if (isNaN(d)) {
        d = new Date(e.textContent);
        if (isNaN(d)) {
            return;
        }
    }
    e.textContent = d.toLocaleString([], {year: "numeric", month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit", second: "2-digit"});
}
var timeList = Array.prototype.slice.call(document.getElementsByTagName("time"));
timeList.forEach(localizeDatetime);
