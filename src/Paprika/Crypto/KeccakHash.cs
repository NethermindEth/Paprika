//  Copyright (c) 2021 Demerzel Solutions Limited
//  This file is part of the Nethermind library.
//
//  The Nethermind library is free software: you can redistribute it and/or modify
//  it under the terms of the GNU Lesser General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
//
//  The Nethermind library is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
//  GNU Lesser General Public License for more details.
//
//  You should have received a copy of the GNU Lesser General Public License
//  along with the Nethermind. If not, see <http://www.gnu.org/licenses/>.

using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using static System.Runtime.CompilerServices.Unsafe;
using static System.Numerics.BitOperations;

// ReSharper disable InconsistentNaming
namespace Paprika.Crypto;

public sealed class KeccakHash
{
    private const int ROUNDS = 24;

    private static readonly ulong[] RoundConstants =
    {
        0x0000000000000001UL, 0x0000000000008082UL, 0x800000000000808aUL,
        0x8000000080008000UL, 0x000000000000808bUL, 0x0000000080000001UL,
        0x8000000080008081UL, 0x8000000000008009UL, 0x000000000000008aUL,
        0x0000000000000088UL, 0x0000000080008009UL, 0x000000008000000aUL,
        0x000000008000808bUL, 0x800000000000008bUL, 0x8000000000008089UL,
        0x8000000000008003UL, 0x8000000000008002UL, 0x8000000000000080UL,
        0x000000000000800aUL, 0x800000008000000aUL, 0x8000000080008081UL,
        0x8000000000008080UL, 0x0000000080000001UL, 0x8000000080008008UL
    };

    // update the state with given number of rounds
    private static void KeccakF(ref KeccakBuffer st)
    {
        ulong bCa, bCe, bCi, bCo, bCu;
        ulong da, de, di, @do, du;
        ulong eba, ebe, ebi, ebo, ebu;
        ulong ega, ege, egi, ego, egu;
        ulong eka, eke, eki, eko, eku;
        ulong ema, eme, emi, emo, emu;
        ulong esa, ese, esi, eso, esu;

        for (int round = 0; round < ROUNDS; round += 2)
        {
            //    prepareTheta
            bCa = st.aba();
            bCe = st.abe();
            bCi = st.abi();
            bCo = st.abo();
            bCu = st.abu();

            bCa ^= st.aga();
            bCe ^= st.age();
            bCi ^= st.agi();
            bCo ^= st.ago();
            bCu ^= st.agu();

            bCa ^= st.aka();
            bCe ^= st.ake();
            bCi ^= st.aki();
            bCo ^= st.ako();
            bCu ^= st.aku();

            bCa ^= st.ama();
            bCe ^= st.ame();
            bCi ^= st.ami();
            bCo ^= st.amo();
            bCu ^= st.amu();

            bCa ^= st.asa();
            bCe ^= st.ase();
            bCi ^= st.asi();
            de = bCa ^ RotateLeft(bCi, 1);
            bCo ^= st.aso();
            di = bCe ^ RotateLeft(bCo, 1);
            bCu ^= st.asu();

            //thetaRhoPiChiIotaPrepareTheta(round  , A, E)
            du = bCo ^ RotateLeft(bCa, 1);
            @do = bCi ^ RotateLeft(bCu, 1);
            da = bCu ^ RotateLeft(bCe, 1);

            bCa = da ^ st.aba();
            bCe = RotateLeft(st.age() ^ de, 44);
            bCi = RotateLeft(st.aki() ^ di, 43);
            eba = (bCa ^ ((~bCe) & bCi)) ^ RoundConstants[round];
            bCo = RotateLeft(st.amo() ^ @do, 21);
            ebe = bCe ^ ((~bCi) & bCo);
            bCu = RotateLeft(st.asu() ^ du, 14);
            ebi = bCi ^ ((~bCo) & bCu);
            ebo = bCo ^ ((~bCu) & bCa);
            ebu = bCu ^ ((~bCa) & bCe);

            bCa = RotateLeft(st.abo() ^ @do, 28);
            bCe = RotateLeft(st.agu() ^ du, 20);
            bCi = RotateLeft(st.aka() ^ da, 3);
            ega = bCa ^ ((~bCe) & bCi);
            bCo = RotateLeft(st.ame() ^ de, 45);
            ege = bCe ^ ((~bCi) & bCo);
            bCu = RotateLeft(st.asi() ^ di, 61);
            egi = bCi ^ ((~bCo) & bCu);
            ego = bCo ^ ((~bCu) & bCa);
            egu = bCu ^ ((~bCa) & bCe);

            st.abe();
            bCa = RotateLeft(st.abe() ^ de, 1);
            bCe = RotateLeft(st.agi() ^ di, 6);
            bCi = RotateLeft(st.ako() ^ @do, 25);
            eka = bCa ^ ((~bCe) & bCi);
            bCo = RotateLeft(st.amu() ^ du, 8);
            eke = bCe ^ ((~bCi) & bCo);
            bCu = RotateLeft(st.asa() ^ da, 18);
            eko = bCo ^ ((~bCu) & bCa);
            eki = bCi ^ ((~bCo) & bCu);
            eku = bCu ^ ((~bCa) & bCe);

            bCa = RotateLeft(st.abu() ^ du, 27);
            bCe = RotateLeft(st.aga() ^ da, 36);
            bCi = RotateLeft(st.ake() ^ de, 10);
            ema = bCa ^ ((~bCe) & bCi);
            bCo = RotateLeft(st.ami() ^ di, 15);
            bCu = RotateLeft(st.aso() ^ @do, 56);
            eme = bCe ^ ((~bCi) & bCo);
            emi = bCi ^ ((~bCo) & bCu);
            emo = bCo ^ ((~bCu) & bCa);
            emu = bCu ^ ((~bCa) & bCe);

            bCa = RotateLeft(st.abi() ^ di, 62);
            bCe = RotateLeft(st.ago() ^ @do, 55);
            bCi = RotateLeft(st.aku() ^ du, 39);
            esa = bCa ^ ((~bCe) & bCi);
            bCo = RotateLeft(st.ama() ^ da, 41);
            ese = bCe ^ ((~bCi) & bCo);
            bCu = RotateLeft(st.ase() ^ de, 2);
            esi = bCi ^ ((~bCo) & bCu);
            eso = bCo ^ ((~bCu) & bCa);
            esu = bCu ^ ((~bCa) & bCe);

            //    prepareTheta
            bCa = eba ^ ega ^ eka ^ ema ^ esa;
            bCe = ebe ^ ege ^ eke ^ eme ^ ese;
            bCi = ebi ^ egi ^ eki ^ emi ^ esi;
            de = bCa ^ RotateLeft(bCi, 1);
            bCo = ebo ^ ego ^ eko ^ emo ^ eso;
            di = bCe ^ RotateLeft(bCo, 1);
            bCu = ebu ^ egu ^ eku ^ emu ^ esu;

            //thetaRhoPiChiIotaPrepareTheta(round+1, E, A)
            du = bCo ^ RotateLeft(bCa, 1);
            da = bCu ^ RotateLeft(bCe, 1);
            @do = bCi ^ RotateLeft(bCu, 1);

            eba = eba ^ da;
            ege = RotateLeft(ege ^ de, 44);
            eki = RotateLeft(eki ^ di, 43);
            st.aba() = (eba ^ ((~ege) & eki)) ^ RoundConstants[round + 1];
            emo = RotateLeft(emo ^ @do, 21);
            st.abe() = ege ^ ((~eki) & emo);
            esu = RotateLeft(esu ^ du, 14);
            st.abi() = eki ^ ((~emo) & esu);
            st.abo() = emo ^ ((~esu) & eba);
            st.abu() = esu ^ ((~eba) & ege);

            ebo = RotateLeft(ebo ^ @do, 28);
            egu = RotateLeft(egu ^ du, 20);
            eka = RotateLeft(eka ^ da, 3);
            st.aga() = ebo ^ ((~egu) & eka);
            eme = RotateLeft(eme ^ de, 45);
            st.age() = egu ^ ((~eka) & eme);
            esi = RotateLeft(esi ^ di, 61);
            st.agi() = eka ^ ((~eme) & esi);
            st.ago() = eme ^ ((~esi) & ebo);
            st.agu() = esi ^ ((~ebo) & egu);

            ebe = RotateLeft(ebe ^ de, 1);
            egi = RotateLeft(egi ^ di, 6);
            eko = RotateLeft(eko ^ @do, 25);
            st.aka() = ebe ^ ((~egi) & eko);
            emu = RotateLeft(emu ^ du, 8);
            st.ake() = egi ^ ((~eko) & emu);
            esa = RotateLeft(esa ^ da, 18);
            st.aki() = eko ^ ((~emu) & esa);
            st.ako() = emu ^ ((~esa) & ebe);
            st.aku() = esa ^ ((~ebe) & egi);

            ebu = RotateLeft(ebu ^ du, 27);
            ega = RotateLeft(ega ^ da, 36);
            eke = RotateLeft(eke ^ de, 10);
            st.ama() = ebu ^ ((~ega) & eke);
            emi = RotateLeft(emi ^ di, 15);
            st.ame() = ega ^ ((~eke) & emi);
            eso = RotateLeft(eso ^ @do, 56);
            st.ami() = eke ^ ((~emi) & eso);
            st.amo() = emi ^ ((~eso) & ebu);
            st.amu() = eso ^ ((~ebu) & ega);

            ebi = RotateLeft(ebi ^ di, 62);
            ego = RotateLeft(ego ^ @do, 55);
            eku = RotateLeft(eku ^ du, 39);
            st.asa() = ebi ^ ((~ego) & eku);
            ema = RotateLeft(ema ^ da, 41);
            st.ase() = ego ^ ((~eku) & ema);
            ese = RotateLeft(ese ^ de, 2);
            st.asi() = eku ^ ((~ema) & ese);
            st.aso() = ema ^ ((~ese) & ebi);
            st.asu() = ese ^ ((~ebi) & ego);
        }
    }

    // compute a Keccak hash (md) of given byte length from "in"
    [SkipLocalsInit]
    public static void ComputeHash(ReadOnlySpan<byte> input, out Keccak output)
    {
        const int roundSize = 136;
        const int ulongRounds = roundSize / sizeof(ulong);

        KeccakBuffer state = default;

        int loopLength = input.Length / roundSize * roundSize;
        if (loopLength > 0)
        {
            ReadOnlySpan<ulong> input64 = MemoryMarshal.Cast<byte, ulong>(input[..loopLength]);
            input = input.Slice(loopLength);
            while (input64.Length > 0)
            {
                for (int i = 0; i < ulongRounds; i++)
                {
                    state[i] ^= input64[i];
                }

                input64 = input64[ulongRounds..];
                KeccakF(ref state);
            }
        }

        Span<byte> temp = stackalloc byte[roundSize];
        temp.Clear();
        input.CopyTo(temp);
        temp[input.Length] = 1;
        temp[roundSize - 1] |= 0x80;

        Span<ulong> tempU64 = MemoryMarshal.Cast<byte, ulong>(temp);
        for (int i = 0; i < tempU64.Length; i++)
        {
            state[i] ^= tempU64[i];
        }

        KeccakF(ref state);
        output = As<KeccakBuffer, Keccak>(ref state);
    }

    [DoesNotReturn]
    private static void ThrowBadKeccak() => throw new ArgumentException("Bad Keccak use");
}

[InlineArray(KeccakBufferCount)]
public struct KeccakBuffer
{
    public const int KeccakBufferCount = 25;
    private ulong st;
}

public static class BufferExtensions
{
    public static ref ulong aba(ref this KeccakBuffer buffer) => ref buffer[0];
    public static ref ulong abe(ref this KeccakBuffer buffer) => ref buffer[1];
    public static ref ulong abi(ref this KeccakBuffer buffer) => ref buffer[2];
    public static ref ulong abo(ref this KeccakBuffer buffer) => ref buffer[3];
    public static ref ulong abu(ref this KeccakBuffer buffer) => ref buffer[4];
    public static ref ulong aga(ref this KeccakBuffer buffer) => ref buffer[5];
    public static ref ulong age(ref this KeccakBuffer buffer) => ref buffer[6];
    public static ref ulong agi(ref this KeccakBuffer buffer) => ref buffer[7];
    public static ref ulong ago(ref this KeccakBuffer buffer) => ref buffer[8];
    public static ref ulong agu(ref this KeccakBuffer buffer) => ref buffer[9];
    public static ref ulong aka(ref this KeccakBuffer buffer) => ref buffer[10];
    public static ref ulong ake(ref this KeccakBuffer buffer) => ref buffer[11];
    public static ref ulong aki(ref this KeccakBuffer buffer) => ref buffer[12];
    public static ref ulong ako(ref this KeccakBuffer buffer) => ref buffer[13];
    public static ref ulong aku(ref this KeccakBuffer buffer) => ref buffer[14];
    public static ref ulong ama(ref this KeccakBuffer buffer) => ref buffer[15];
    public static ref ulong ame(ref this KeccakBuffer buffer) => ref buffer[16];
    public static ref ulong ami(ref this KeccakBuffer buffer) => ref buffer[17];
    public static ref ulong amo(ref this KeccakBuffer buffer) => ref buffer[18];
    public static ref ulong amu(ref this KeccakBuffer buffer) => ref buffer[19];
    public static ref ulong asa(ref this KeccakBuffer buffer) => ref buffer[20];
    public static ref ulong ase(ref this KeccakBuffer buffer) => ref buffer[21];
    public static ref ulong asi(ref this KeccakBuffer buffer) => ref buffer[22];
    public static ref ulong aso(ref this KeccakBuffer buffer) => ref buffer[23];
    public static ref ulong asu(ref this KeccakBuffer buffer) => ref buffer[24];
}
