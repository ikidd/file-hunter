const STORAGE_KEY = 'fh-activity-log-open';

function ts() {
    const d = new Date();
    const hh = String(d.getHours()).padStart(2, '0');
    const mm = String(d.getMinutes()).padStart(2, '0');
    const ss = String(d.getSeconds()).padStart(2, '0');
    return `${hh}:${mm}:${ss}`;
}

const ActivityLog = {
    el: null,
    listEl: null,
    toggleEl: null,
    open: false,

    init() {
        this.el = document.getElementById('activity-log');
        this.listEl = document.getElementById('activity-log-list');
        this.toggleEl = document.getElementById('activity-log-toggle');

        this.open = localStorage.getItem(STORAGE_KEY) !== 'false';
        this._apply();

        this.toggleEl.addEventListener('click', () => {
            this.open = !this.open;
            localStorage.setItem(STORAGE_KEY, this.open);
            this._apply();
        });
    },

    _apply() {
        this.el.classList.toggle('collapsed', !this.open);
        this.toggleEl.textContent = this.open ? 'Activity \u25BE' : 'Activity \u25B8';
    },

    add(text) {
        const row = document.createElement('div');
        row.className = 'activity-entry';
        row.innerHTML = `<span class="activity-ts">${ts()}</span> ${text}`;
        this.listEl.appendChild(row);
        while (this.listEl.childElementCount > 100) {
            this.listEl.removeChild(this.listEl.firstElementChild);
        }
        this.listEl.scrollTop = this.listEl.scrollHeight;
    },
};

export default ActivityLog;
