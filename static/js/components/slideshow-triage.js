import API from '../api.js';
import Toast from './toast.js';
import icons from '../icons.js';

const SlideshowTriage = {
    // Delete dialog elements
    _delOverlay: null,
    _delText: null,
    _delList: null,
    _delDupsCheck: null,
    _delCancel: null,
    _delSubmit: null,

    // Consolidate dialog elements
    _conOverlay: null,
    _conText: null,
    _conList: null,
    _conTree: null,
    _conDest: null,
    _conCancel: null,
    _conSubmit: null,

    // Tag dialog elements
    _tagOverlay: null,
    _tagText: null,
    _tagList: null,
    _tagInput: null,
    _tagCancel: null,
    _tagSubmit: null,

    // State
    _deleteItems: [],
    _consolidateItems: [],
    _tagItems: [],
    _treeData: null,
    _expandedNodes: new Set(),
    _selectedDest: null,

    init() {
        // Delete dialog
        this._delOverlay = document.getElementById('slideshow-delete-modal');
        this._delText = document.getElementById('slideshow-delete-text');
        this._delList = document.getElementById('slideshow-delete-list');
        this._delDupsCheck = document.getElementById('slideshow-delete-dups-check');
        this._delCancel = document.getElementById('slideshow-delete-cancel');
        this._delSubmit = document.getElementById('slideshow-delete-submit');

        this._delCancel.addEventListener('click', () => this._closeDelete());
        this._delOverlay.addEventListener('click', (e) => {
            if (e.target === this._delOverlay) this._closeDelete();
        });
        this._delSubmit.addEventListener('click', () => this._doDelete());

        // Consolidate dialog
        this._conOverlay = document.getElementById('slideshow-consolidate-modal');
        this._conText = document.getElementById('slideshow-consolidate-text');
        this._conList = document.getElementById('slideshow-consolidate-list');
        this._conTree = document.getElementById('slideshow-consolidate-tree');
        this._conDest = document.getElementById('slideshow-consolidate-dest');
        this._conCancel = document.getElementById('slideshow-consolidate-cancel');
        this._conSubmit = document.getElementById('slideshow-consolidate-submit');

        this._conCancel.addEventListener('click', () => this._closeConsolidate());
        this._conOverlay.addEventListener('click', (e) => {
            if (e.target === this._conOverlay) this._closeConsolidate();
        });
        this._conSubmit.addEventListener('click', () => this._doConsolidate());

        // Tag dialog
        this._tagOverlay = document.getElementById('slideshow-tag-modal');
        this._tagText = document.getElementById('slideshow-tag-text');
        this._tagList = document.getElementById('slideshow-tag-list');
        this._tagInput = document.getElementById('slideshow-tag-input');
        this._tagCancel = document.getElementById('slideshow-tag-cancel');
        this._tagSubmit = document.getElementById('slideshow-tag-submit');

        this._tagCancel.addEventListener('click', () => this._closeTag());
        this._tagOverlay.addEventListener('click', (e) => {
            if (e.target === this._tagOverlay) this._closeTag();
        });
        this._tagSubmit.addEventListener('click', () => this._doTag());

        // Escape key for all dialogs
        document.addEventListener('keydown', (e) => {
            if (e.key !== 'Escape') return;
            if (!this._delOverlay.classList.contains('hidden')) {
                this._closeDelete();
            } else if (!this._conOverlay.classList.contains('hidden')) {
                this._closeConsolidate();
            } else if (!this._tagOverlay.classList.contains('hidden')) {
                this._closeTag();
            }
        });
    },

    show(deleteItems, consolidateItems, tagItems) {
        this._deleteItems = deleteItems || [];
        this._consolidateItems = consolidateItems || [];
        this._tagItems = tagItems || [];

        this._showNext();
    },

    _showNext() {
        if (this._deleteItems.length > 0) {
            this._showDeleteDialog();
        } else if (this._consolidateItems.length > 0) {
            this._showConsolidateDialog();
        } else if (this._tagItems.length > 0) {
            this._showTagDialog();
        } else {
            this._finish();
        }
    },

    // ── Capped file list ──

    _renderCappedList(container, items) {
        container.innerHTML = '';
        const max = 5;
        const shown = items.slice(0, max);
        for (const item of shown) {
            const div = document.createElement('div');
            div.textContent = item.name;
            container.appendChild(div);
        }
        if (items.length > max) {
            const more = document.createElement('div');
            more.textContent = `...and ${items.length - max} more`;
            more.style.opacity = '0.5';
            container.appendChild(more);
        }
    },

    // ── Delete dialog ──

    _showDeleteDialog() {
        const n = this._deleteItems.length;
        this._delText.textContent = `Delete ${n} image${n !== 1 ? 's' : ''}? Files will be removed from disk and the catalog.`;
        this._renderCappedList(this._delList, this._deleteItems);
        this._delDupsCheck.checked = true;
        this._delSubmit.textContent = 'Delete';
        this._delSubmit.disabled = false;
        this._delOverlay.classList.remove('hidden');
    },

    _closeDelete() {
        this._delOverlay.classList.add('hidden');
        this._deleteItems = [];
        this._showNext();
    },

    _doDelete() {
        const allDups = this._delDupsCheck.checked;
        const fileIds = this._deleteItems.map(item => item.id);
        const n = fileIds.length;

        // Fire-and-forget — WS batch_deleted handles UI refresh
        API.post('/api/batch/delete', { file_ids: fileIds, all_duplicates: allDups });
        Toast.info(`Deleting ${n} image${n !== 1 ? 's' : ''}...`);

        this._delOverlay.classList.add('hidden');
        this._deleteItems = [];
        this._showNext();
    },

    // ── Consolidate dialog ──

    async _showConsolidateDialog() {
        const n = this._consolidateItems.length;
        this._conText.textContent = `Consolidate ${n} image${n !== 1 ? 's' : ''} to a single location.`;
        this._renderCappedList(this._conList, this._consolidateItems);

        this._selectedDest = null;
        this._expandedNodes = new Set();
        this._conDest.textContent = 'No folder selected';
        this._conSubmit.textContent = 'Consolidate';
        this._conSubmit.disabled = false;

        const res = await API.get('/api/locations');
        this._treeData = res.ok ? res.data : [];
        this._renderTree();

        this._conOverlay.classList.remove('hidden');
    },

    _closeConsolidate() {
        this._conOverlay.classList.add('hidden');
        this._consolidateItems = [];
        this._showNext();
    },

    _doConsolidate() {
        if (!this._selectedDest) return;
        const fileIds = this._consolidateItems.map(item => item.id);
        const n = fileIds.length;

        // Fire-and-forget — WS consolidate_completed/batch_consolidate_completed handle UI
        API.post('/api/batch/consolidate', {
            file_ids: fileIds,
            mode: 'copy_to',
            destination_folder_id: this._selectedDest,
        });
        Toast.info(`Consolidating ${n} image${n !== 1 ? 's' : ''}...`);

        this._conOverlay.classList.add('hidden');
        this._consolidateItems = [];
        this._showNext();
    },

    // ── Tag dialog ──

    _showTagDialog() {
        const n = this._tagItems.length;
        this._tagText.textContent = `Tag ${n} image${n !== 1 ? 's' : ''}.`;
        this._renderCappedList(this._tagList, this._tagItems);
        this._tagInput.value = '';
        this._tagSubmit.textContent = 'Tag';
        this._tagSubmit.disabled = false;
        this._tagOverlay.classList.remove('hidden');
        this._tagInput.focus();
    },

    _closeTag() {
        this._tagOverlay.classList.add('hidden');
        this._tagItems = [];
        this._showNext();
    },

    _doTag() {
        const tag = this._tagInput.value.trim();
        if (!tag) return;
        const fileIds = this._tagItems.map(item => item.id);
        const n = fileIds.length;

        API.post('/api/batch/tag', { file_ids: fileIds, add_tags: [tag] });
        Toast.info(`Tagging ${n} image${n !== 1 ? 's' : ''} with "${tag}"`);

        this._tagOverlay.classList.add('hidden');
        this._tagItems = [];
        this._showNext();
    },

    _finish() {
        this._deleteItems = [];
        this._consolidateItems = [];
        this._tagItems = [];
    },

    // ── Tree picker (same pattern as consolidate.js) ──

    _renderTree() {
        this._conTree.innerHTML = '';
        if (!this._treeData) return;
        this._treeData.forEach(loc => {
            this._renderTreeNode(this._conTree, loc, 0);
        });
    },

    _renderTreeNode(container, node, depth) {
        const div = document.createElement('div');
        div.className = 'ct-node';
        if (node.online === false) div.classList.add('ct-offline');
        if (this._selectedDest === node.id) div.classList.add('ct-selected');

        for (let i = 0; i < depth; i++) {
            const indent = document.createElement('span');
            indent.className = 'ct-indent';
            div.appendChild(indent);
        }

        const hasChildren = node.hasChildren || (node.children && node.children.length > 0);
        const toggle = document.createElement('span');
        toggle.className = 'ct-icon';
        if (hasChildren) {
            toggle.textContent = this._expandedNodes.has(node.id) ? '\u25BE' : '\u25B8';
        }
        div.appendChild(toggle);

        const icon = document.createElement('span');
        icon.className = 'ct-icon';
        icon.innerHTML = node.type === 'location' ? icons.location : icons.folder;
        div.appendChild(icon);

        const label = document.createElement('span');
        label.className = 'ct-label';
        label.textContent = node.label;
        div.appendChild(label);

        div.addEventListener('click', async (e) => {
            e.stopPropagation();
            if (node.online === false) return;

            if (hasChildren) {
                if (this._expandedNodes.has(node.id)) {
                    this._expandedNodes.delete(node.id);
                } else {
                    this._expandedNodes.add(node.id);
                    if (node.children === null) {
                        const numId = node.id.replace('fld-', '');
                        const res = await API.get(`/api/tree/children?ids=${numId}`);
                        if (res.ok && res.data[node.id]) {
                            node.children = res.data[node.id];
                        } else {
                            node.children = [];
                        }
                    }
                }
            }
            this._selectedDest = node.id;
            this._conDest.textContent = node.label;
            this._renderTree();
        });

        container.appendChild(div);

        if (node.children && node.children.length > 0 && this._expandedNodes.has(node.id)) {
            node.children.forEach(child => this._renderTreeNode(container, child, depth + 1));
        }
    },
};

export default SlideshowTriage;
