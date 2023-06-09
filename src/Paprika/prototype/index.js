const root = document.getElementById('root')
const canvas = document.getElementById('canvas')
const ctx = canvas.getContext('2d')

const updateBtn = document.getElementById('updateBtn')
const updatePath = document.getElementById('updatePath')
const updateValue = document.getElementById('updateValue')

const rootHashBtn = document.getElementById('rootHashBtn')
function displayRootHash(value) { root.innerText = value }

const NODE_WIDTH = 200
const NODE_HEIGHT = 50
const PADDING = 10
const LINE_WIDTH = 2

const DELAY = 500

function delay() {
    return new Promise((res, _) => {
        setTimeout(() => { res() }, DELAY);
    })
}

class Db {
    constructor(ctx) {
        this._ctx = ctx
        this._rootPtr = {}
    }

    get(path) {
        let nibbles = path.split('')
        let ptr = this._rootPtr
        for (let nibble of nibbles) {
            ptr = ptr[nibble]
        }
        return ptr.node
    }

    set(path, node) {
        let nibbles = path.split('')
        let ptr = this._rootPtr
        for (let nibble of nibbles) {
            if (!ptr[nibble]) { ptr[nibble] = {} }
            ptr = ptr[nibble]
        }
        ptr.node = node
    }

    async updateValue(path, value) {
        let ptr = this._rootPtr
        for (let nibble of path.split('')) {
            if (ptr.node) {
                ptr.node._dirty = true
            }
            ptr = ptr[nibble]
        }

        if (!ptr) { throw new Error(`No node on path ${path}`) }
        if (!(ptr.node instanceof Leaf)) { throw new Error(`Can't update non-Leaf node`) }
        ptr.node._dirty = true
        ptr.node._value = value
    }

    async rootHash() {
        // We compute the root hash starting on the empty prefix
        let rootNode = this.get('')
        if (rootNode) {
            return await rootNode.hash('', this)
        } else {
            return "<unknown>"
        }
    }

    allNodes() {
        let nodes = []
        function rec(path, ptr) {
            if (!ptr) { return }
            if (ptr.node) { nodes.push([path, ptr.node]) }

            for (let [k, v] of Object.entries(ptr)) {
                if (k == 'node') { continue }
                rec(path + k, v)
            }
        }
        rec('', this._rootPtr)

        return nodes
    }

    displayNodes() {
        this._ctx.clearRect(0, 0, 500, 500)

        let offsets = { x: -1, y: 0 }
        for (let [path, node] of this.allNodes()) {
            let depth = path.length

            if (offsets.y < depth) {
                offsets.x = 0;
                offsets.y = depth
            } else {
                offsets.x++;
            }

            let x = PADDING + offsets.x * (NODE_WIDTH + PADDING)
            let y = PADDING + (offsets.y * (NODE_HEIGHT + PADDING))

            this._ctx.fillStyle = node.color()
            this._ctx.lineWidth = LINE_WIDTH
            this._ctx.fillRect(x, y, NODE_WIDTH, NODE_HEIGHT)

            if (node._dirty) {
                this._ctx.strokeStyle = 'red';
                this._ctx.strokeRect(x, y, NODE_WIDTH, NODE_HEIGHT)
            }

            this._ctx.font = `${NODE_HEIGHT / 3}px Arial`
            this._ctx.fillStyle = 'black'
            let text = `/${path.split('').join('/')} : ${node.toString()}`
            this._ctx.fillText(text, PADDING + x, (NODE_HEIGHT + PADDING) / 2 + y)
        }
    }
}

class Node {
    constructor(value, onHash) {
        this._value = value
        this._dirty = true
        this._hash = undefined
        this._onHash = onHash
    }

    async computeHash(path, db) { throw new Error('Abstract method not implemented') }

    async hash(path, db) {
        if (!this._dirty) { return this._hash }
        this._hash = await this.computeHash(path, db)
        this._dirty = false
        await this._onHash()
        return this._hash
    }
}

class Leaf extends Node {
    toString() { return `"${this._value}"` }

    color() { return 'rgb(200, 190, 220)' }

    async computeHash(path, db) {
        return `h(${path} -> ${this._value})`
    }
}

class Extension extends Node {
    toString() { return `<${this._value}>` }

    color() { return 'rgb(215, 230, 190)' }

    async computeHash(path, db) {
        let childPath = path + this._value
        let child = db.get(childPath)
        let childHash = await child.hash(childPath, db)
        return `h(${childHash})`
    }
}

class Branch extends Node {
    toString() { return `[${this._value}]` }

    color() { return 'rgb(180, 220, 240)' }

    async computeHash(path, db) {
        let childrenHashes = []
        for (let i = 0; i < this._value.length; i++) {
            let nibble = this._value[i]

            let childPath = path + nibble
            let child = db.get(childPath)

            let childHash = await child.hash(childPath, db)
            childrenHashes.push(childHash)
        }
        return `h(${childrenHashes.join(' + ')})`
    }
}

function mkNode({ type, value }, onHash) {
    switch (type) {
        case 'leaf': return new Leaf(value, onHash)
        case 'branch': return new Branch(value, onHash)
        case 'extension': return new Extension(value, onHash)
    }
}

async function main() {
    /*
    Storage is:
    ABC -> "Hello"
    ABD -> "World"

    Full tree structure:
    0 -> Extension; AB
    AB -> Branch; [C, D]
    ABC -> Leaf; "Hello"
    ABD -> Leaf; "World"
    */
    const db = new Db(ctx)
    const onHash = async () => {
        await delay()
        db.displayNodes()
    }
    db.set('', mkNode({ type: 'extension', value: 'AB' }, onHash))
    db.set('AB', mkNode({ type: 'branch', value: ['C', 'D'] }, onHash))
    db.set('ABC', mkNode({ type: 'leaf', value: 'Hello' }, onHash))
    db.set('ABD', mkNode({ type: 'leaf', value: 'World' }, onHash))

    db.displayNodes()
    displayRootHash('<unknown>')

    updateBtn.onclick = async () => {
        db.updateValue(updatePath.value, updateValue.value)
        updatePath.value = ''
        updateValue.value = ''
        db.displayNodes()
    }

    rootHashBtn.onclick = async () => {
        displayRootHash(await db.rootHash())
    }
}

main()
