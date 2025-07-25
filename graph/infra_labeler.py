
def get_known_infra_addresses() -> set[str]:
    return {
        "0x68b3465833fb72a70ecdf485e0e4c7bd8665fc45",  # Uniswap V3 Router
        "0x7a250d5630b4cf539739df2c5dacb4c659f2488d",  # Uniswap V2 Router
        "0x1111111254eeb25477b68fb85ed929f73a960582",  # 1inch Router V4
        "0xdef1c0ded9bec7f1a1670819833240f027b25eff",  # 0x: Exchange Proxy
        "0x00000000006c3852cbef3e08e8df289169ede581",  # OpenSea Seaport 1.1
        "0x0000a26b00c1f0df003000390027140000faa719",  # OpenSea Seaport 1.4
        "0x000000000000ad05ccc4f10045630fb830b95127",  # Blur.io Marketplace
        "0x0000000000a39bb272e79075ade125fd351887ac",  # OpenSea shared store
        "0x70423fc4400c3dc01168ebe23f68d0f3ee9a0123",  # Blur.io Execution
        "0x74312363e45dcaba76c59ec49a7aa8a65a67eed3",  # X2Y2 Marketplace
        "0x28c6c06298d514db089934071355e5743bf21d60",  # Binance 14
        "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",  # WETH contract
        "0x74de5d4fcbf63e00296fd95d33236b9794016631",  # CowSwap Settlement
        "0xa9d1e08c7793af67e9d92fe308d5697fb81d3e43",  # Blur Blend
        "0xae45a8240147e6179ec7c9f92c5a18f9a97b3fca",  # Blur Lending
        "0x21a31ee1afc51d94c2efccaa2092ad1028285549",  # Binance 14
        "0x4976a4a02f38326660d17bf34b431dc6e2eb2327",  # Gem V2
        "0x9696f59e4d72e237be84ffd425dcad154bf96976",  # Blur exchange
        "0x56eddb7aa87536c09ccc2793473599fd21a8b17f",  # Binance 8
        "0xdfd5293d8e347dfe59e90efd55b2956a1343963d",  # Binance 7
        "0x0b95993a39a363d99280ac950f5e4536ab5c5566",  # OpenSea/Seaport
        "0x46340b20830761efd32832a74d7169b29feb9758",  # Coinbase hot wallet
        
    }


# 📘 地址說明（可用於標註、解釋、debug log）
def describe_address(address: str) -> str:
    address = address.lower()
    desc = {
            "0x68b3465833fb72a70ecdf485e0e4c7bd8665fc45": "Uniswap V3 Router",
            "0x7a250d5630b4cf539739df2c5dacb4c659f2488d": "Uniswap V2 Router",
            "0x1111111254eeb25477b68fb85ed929f73a960582": "1inch Router V4",
            "0xdef1c0ded9bec7f1a1670819833240f027b25eff": "0x Exchange Proxy",
            "0x00000000006c3852cbef3e08e8df289169ede581": "OpenSea Seaport 1.1",
            "0x0000a26b00c1f0df003000390027140000faa719": "OpenSea Seaport 1.4",
            "0x000000000000ad05ccc4f10045630fb830b95127": "Blur Marketplace",
            "0x0000000000a39bb272e79075ade125fd351887ac": "OpenSea Shared Store",
            "0x70423fc4400c3dc01168ebe23f68d0f3ee9a0123": "Blur Execution",
            "0x74312363e45dcaba76c59ec49a7aa8a65a67eed3": "X2Y2 Marketplace",
            "0x28c6c06298d514db089934071355e5743bf21d60": "Binance 14",
            "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2": "WETH Token",
            "0x74de5d4fcbf63e00296fd95d33236b9794016631": "CowSwap Settlement",
            "0xa9d1e08c7793af67e9d92fe308d5697fb81d3e43": "Blur Blend",
            "0xae45a8240147e6179ec7c9f92c5a18f9a97b3fca": "Blur Lending",
            "0x21a31ee1afc51d94c2efccaa2092ad1028285549": "Binance 14",
            "0x4976a4a02f38326660d17bf34b431dc6e2eb2327": "Gem V2",
            "0x9696f59e4d72e237be84ffd425dcad154bf96976": "Blur Exchange",
            "0x56eddb7aa87536c09ccc2793473599fd21a8b17f": "Binance 8",
            "0xdfd5293d8e347dfe59e90efd55b2956a1343963d": "Binance 7",
            "0x0b95993a39a363d99280ac950f5e4536ab5c5566": "OpenSea/Seaport",
            "0x46340b20830761efd32832a74d7169b29feb9758": "Coinbase Hot Wallet",
    }
    return desc.get(address, "Unknown infra")