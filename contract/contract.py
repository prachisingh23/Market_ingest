import json
import pickle
import requests
import datetime
import gzip
import io
import pandas as pd
from pathlib import Path


def load_contracts(data):
    """Load and process contract data"""

    def sort_fut_opt_exp(fut_date, opt_date):
        """
        Assigns expiry labels:
          - For futures: I, II, III, ...
          - For options: 1, 2, 3, ...
        """
        fut = {}
        for symbol in fut_date:
            if symbol not in fut:
                fut[symbol] = {}
                a = sorted(fut_date[symbol])
                for i in range(len(a)):
                    if i == 0:
                        fut[symbol][a[i].strftime('%Y%m%d')] = 'I'
                    elif i == 1:
                        fut[symbol][a[i].strftime('%Y%m%d')] = 'II'
                    elif i == 2:
                        fut[symbol][a[i].strftime('%Y%m%d')] = 'III'
                    elif i == 3:
                        fut[symbol][a[i].strftime('%Y%m%d')] = 'IV'
                    elif i == 4:
                        fut[symbol][a[i].strftime('%Y%m%d')] = 'V'

        opt = {}
        for symbol in opt_date:
            if symbol not in opt:
                opt[symbol] = {}
                a = sorted(opt_date[symbol])
                for i in range(len(a)):
                    opt[symbol][a[i].strftime('%Y%m%d')] = i + 1

        return fut, opt

    contract_dict = {}
    cash_dict = {}
    fut_date = {}
    opt_date = {}
    spot_dict = {}
    trading_symbol = {}
    new_contract_dict = {}

    segment = ['NSE_FO', 'BSE_FO', 'NSE_INDEX', 'BSE_INDEX', 'NSE_EQ', 'BSE_EQ']

    for i in data:
        seg = i.get('segment')
        instrument_type = i.get('instrument_type')  # CE, PE, FUT, INDEX, etc.

        if seg in segment:
            token = i.get('exchange_token')
            if token is None:
                continue

            if seg not in contract_dict:
                contract_dict[seg] = {}
            contract_dict[seg][token] = i

            if instrument_type in ['CE', 'PE', 'FUT']:
                symbol = i.get('asset_symbol')
                if not symbol:
                    continue

                expiry_epoch = int(i.get('expiry', 0)) / 1000
                expiry_date = datetime.datetime.fromtimestamp(expiry_epoch)

                # Build FUT/OPT expiry maps
                if 'FUT' in instrument_type:
                    fut_date.setdefault(symbol, [])
                    if expiry_date not in fut_date[symbol]:
                        fut_date[symbol].append(expiry_date)

                elif instrument_type in ['PE', 'CE']:
                    opt_date.setdefault(symbol, [])
                    if expiry_date not in opt_date[symbol]:
                        opt_date[symbol].append(expiry_date)

            elif instrument_type == 'INDEX':
                symbol = i.get('name')
                trading_symbol_name = i.get('trading_symbol')
                spot_dict[token] = {
                    'type': instrument_type,
                    'symbol': symbol,
                    'trading_symbol': trading_symbol_name,
                    'info': i,
                }

            elif seg in ['NSE_EQ', 'BSE_EQ']:
                symbol = i.get('trading_symbol')
                trading_symbol_name = i.get('trading_symbol')
                cash_dict[token] = {
                    'type': instrument_type,
                    'symbol': symbol,
                    'trading_symbol': trading_symbol_name,
                    'info': i
                }

        # trading_symbol map (segment -> trading_symbol_name -> token)
        if seg not in trading_symbol:
            trading_symbol[seg] = {}
        if seg:
            trading_symbol_name = i.get('trading_symbol')
            token = i.get('exchange_token')
            if trading_symbol_name and token:
                trading_symbol[seg][trading_symbol_name] = token

    fut_date, opt_date = sort_fut_opt_exp(fut_date, opt_date)

    # ----------------------------
    # Create new contract dictionary
    # ----------------------------
    for seg in ['NSE_FO', 'BSE_FO']:
        if seg not in contract_dict:
            continue

        for token in contract_dict[seg]:
            info = contract_dict[seg][token]
            instrument_type = info.get('instrument_type')
            symbol = info.get('asset_symbol')

            expiry_epoch = int(info.get('expiry', 0)) / 1000
            expiry_date = datetime.datetime.fromtimestamp(expiry_epoch)

            if instrument_type == 'FUT':
                expiry_dt = expiry_date.strftime('%Y%m%d')

                fut_code = None
                for exp, series in fut_date.get(symbol, {}).items():
                    if exp == expiry_dt:
                        fut_code = series
                        break

                if fut_code is None:
                    continue

                token_symbol = f"{symbol}-{fut_code}"
                new_contract_dict[token_symbol] = {'token': token}

            elif instrument_type in ['CE', 'PE']:
                strike = info.get('strike_price')
                option_type = instrument_type
                expiry_dt = expiry_date.strftime('%Y%m%d')

                week = None
                for exp, series in opt_date.get(symbol, {}).items():
                    if exp == expiry_dt:
                        week = f"W{series}"
                        break

                if week is None:
                    continue

                token_symbol = f"{symbol}-{week}-{strike}-{option_type}"
                new_contract_dict[token_symbol] = {'token': token, 'seg': seg}

    for token, info in spot_dict.items():
        ts = info.get('trading_symbol')
        if ts:
            new_contract_dict[ts] = {'token': token}

    for token, info in cash_dict.items():
        ts = info.get('trading_symbol')
        if not ts:
            continue

        if ts not in new_contract_dict:
            new_contract_dict[ts] = {}

        exchange_name = info.get('info', {}).get('exchange')
        if exchange_name and exchange_name not in new_contract_dict[ts]:
            new_contract_dict[ts][exchange_name] = {'token': token}

    return contract_dict, fut_date, opt_date, spot_dict, cash_dict, new_contract_dict, trading_symbol


def check_contract_file(data):
    all_headers = set()
    for item in data:
        all_headers.update(item.keys())

    all_headers = sorted(list(all_headers))

    normalized = []
    for item in data:
        row = {key: item.get(key, "") for key in all_headers}
        normalized.append(row)

    df = pd.DataFrame(normalized)
    df.to_csv("universal_contracts.csv", index=False)


def contract_file(force_refresh: bool = True):
    """
    Build (or load) the Upstox contract cache and token list.

    Args:
        force_refresh:
            True  -> always download and regenerate contract.pkl + tokens.txt
            False -> if contract.pkl exists, load it instead of downloading again
    """

    contract_dir = Path(__file__).resolve().parent  # .../contract
    pkl_path = contract_dir / "contract.pkl"
    tokens_path = contract_dir / "tokens.txt"

    # OPTIONAL CACHE LOAD
    if not force_refresh and pkl_path.exists():
        with open(pkl_path, "rb") as f:
            main = pickle.load(f)
        print(f"✅ Loaded cached contract file: {pkl_path}")
        return main

    url = "https://assets.upstox.com/market-quote/instruments/exchange/complete.json.gz"
    response = requests.get(url, timeout=60)
    response.raise_for_status()

    with gzip.GzipFile(fileobj=io.BytesIO(response.content)) as gz:
        data = json.loads(gz.read().decode('utf-8'))

    contract_dict, fut_date, opt_date, spot_dict, cash_dict, new_contract_dict, trading_symbol = load_contracts(data)

    main = {
        'contract_dict': contract_dict,
        'fut_date': fut_date,
        'opt_date': opt_date,
        'spot_dict': spot_dict,
        'cash_dict': cash_dict,
        'new_contract_dict': new_contract_dict,
        'trading_symbol': trading_symbol,
    }

    # Write contract.pkl (portable path)
    with open(pkl_path, "wb") as g:
        pickle.dump(main, g)

    # print(f"Contract loading complete. Saved: {pkl_path}")

    # Write tokens.txt (portable path)
    symbols = ['NIFTY', 'SENSEX', 'BANKEX', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY']

    with open(tokens_path, "w") as tokens:
        tokens.write(
            "BSE_INDEX|SENSEX\n"
            "NSE_INDEX|Nifty 50\n"
            "BSE_INDEX|BANKEX\n"
            "NSE_INDEX|Nifty Bank\n"
            "NSE_INDEX|Nifty Fin Service\n"
            "NSE_INDEX|NIFTY MID SELECT\n"
        )

        for k in new_contract_dict:
            line = k.split('-')
            if 'seg' in new_contract_dict[k]:
                for sym in symbols:
                    if len(line) >= 2 and (sym + '-W1') == (line[0] + '-' + line[1]):
                        seg = new_contract_dict[k]['seg']
                        tok = new_contract_dict[k]['token']
                        info = contract_dict[seg][tok]
                        tokens.write(info['instrument_key'] + '\n')

    # print(f"Tokens written: {tokens_path}")
    return main


# Example usage:
contract = contract_file(force_refresh=True)
